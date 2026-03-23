/**
 * Ingestion crawler for the AdC (Autoridade da Concorrencia) MCP server.
 *
 * Scrapes competition decisions, merger control decisions, and sector data
 * from concorrencia.pt and populates the SQLite database.
 *
 * Data sources:
 *   - AdC news/press releases listing (concorrencia.pt/pt/noticias-comunicados-e-intervencoes)
 *     Paginated with ?page=N (10 articles per page). Articles contain decision
 *     announcements, merger notifications, sector inquiries, and sanction press releases.
 *   - Individual article pages (/pt/artigos/[slug]) with decision details, case numbers,
 *     parties, fine amounts, and links to PDF decisions.
 *   - PesquisAdC case reference links (extranet.concorrencia.pt/PesquisAdC/CCENT.aspx?REf=...)
 *
 * Case number conventions:
 *   - PRC/YYYY/NN  — Processo de Contraordenacao (enforcement / restrictive practices)
 *   - Ccent/YYYY/NN or Ccent_YYYY_NN — Controlo de Concentracoes (merger control)
 *   - INQ/YYYY/NN  — Inquerito Setorial (sector inquiry)
 *
 * Usage:
 *   npx tsx scripts/ingest-adc.ts
 *   npx tsx scripts/ingest-adc.ts --dry-run
 *   npx tsx scripts/ingest-adc.ts --resume
 *   npx tsx scripts/ingest-adc.ts --force
 *   npx tsx scripts/ingest-adc.ts --max-pages 5
 */

import Database from "better-sqlite3";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { dirname, join } from "node:path";
import * as cheerio from "cheerio";
import { SCHEMA_SQL } from "../src/db.js";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const DB_PATH = process.env["ADC_DB_PATH"] ?? "data/adc.db";
const STATE_FILE = join(dirname(DB_PATH), "ingest-state.json");
const BASE_URL = "https://www.concorrencia.pt";
const LISTING_PATH = "/pt/noticias-comunicados-e-intervencoes";
const RATE_LIMIT_MS = 1500;
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 5000;
const USER_AGENT =
  "AnsvarAdCCrawler/1.0 (+https://github.com/Ansvar-Systems/portuguese-competition-mcp)";

// CLI flags
const dryRun = process.argv.includes("--dry-run");
const resume = process.argv.includes("--resume");
const force = process.argv.includes("--force");
const maxPagesArg = process.argv.find((_, i, a) => a[i - 1] === "--max-pages");
const maxPages = maxPagesArg ? parseInt(maxPagesArg, 10) : Infinity;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface IngestState {
  processedUrls: string[];
  lastRun: string;
  decisionsIngested: number;
  mergersIngested: number;
  errors: string[];
}

interface ListingEntry {
  url: string;
  title: string;
  date: string | null;
  type: string; // "comunicados" | "noticias" | "intervencoes"
}

interface ParsedDecision {
  case_number: string;
  title: string;
  date: string | null;
  type: string | null;
  sector: string | null;
  parties: string | null;
  summary: string | null;
  full_text: string;
  outcome: string | null;
  fine_amount: number | null;
  gwb_articles: string | null;
  status: string;
}

interface ParsedMerger {
  case_number: string;
  title: string;
  date: string | null;
  sector: string | null;
  acquiring_party: string | null;
  target: string | null;
  summary: string | null;
  full_text: string;
  outcome: string | null;
  turnover: number | null;
}

interface SectorAccumulator {
  [id: string]: {
    name: string;
    name_en: string | null;
    description: string | null;
    decisionCount: number;
    mergerCount: number;
  };
}

// ---------------------------------------------------------------------------
// HTTP fetching with rate limiting and retries
// ---------------------------------------------------------------------------

let lastRequestTime = 0;

async function rateLimitedFetch(url: string): Promise<string | null> {
  const now = Date.now();
  const elapsed = now - lastRequestTime;
  if (elapsed < RATE_LIMIT_MS) {
    await sleep(RATE_LIMIT_MS - elapsed);
  }

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      lastRequestTime = Date.now();
      const response = await fetch(url, {
        headers: {
          "User-Agent": USER_AGENT,
          Accept:
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          "Accept-Language": "pt-PT,pt;q=0.9,en;q=0.5",
        },
        redirect: "follow",
        signal: AbortSignal.timeout(30_000),
      });

      if (response.status === 403 || response.status === 429) {
        console.warn(
          `  [WARN] HTTP ${response.status} for ${url} (attempt ${attempt}/${MAX_RETRIES})`,
        );
        if (attempt < MAX_RETRIES) {
          await sleep(RETRY_DELAY_MS * attempt);
          continue;
        }
        return null;
      }

      if (!response.ok) {
        console.warn(`  [WARN] HTTP ${response.status} for ${url}`);
        return null;
      }

      return await response.text();
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      console.warn(
        `  [WARN] Fetch error for ${url} (attempt ${attempt}/${MAX_RETRIES}): ${message}`,
      );
      if (attempt < MAX_RETRIES) {
        await sleep(RETRY_DELAY_MS * attempt);
      }
    }
  }

  return null;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ---------------------------------------------------------------------------
// State management (for --resume)
// ---------------------------------------------------------------------------

function loadState(): IngestState {
  if (resume && existsSync(STATE_FILE)) {
    try {
      const raw = readFileSync(STATE_FILE, "utf-8");
      return JSON.parse(raw) as IngestState;
    } catch {
      console.warn("[WARN] Could not read state file, starting fresh.");
    }
  }
  return {
    processedUrls: [],
    lastRun: new Date().toISOString(),
    decisionsIngested: 0,
    mergersIngested: 0,
    errors: [],
  };
}

function saveState(state: IngestState): void {
  state.lastRun = new Date().toISOString();
  const dir = dirname(STATE_FILE);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
  }
  writeFileSync(STATE_FILE, JSON.stringify(state, null, 2), "utf-8");
}

// ---------------------------------------------------------------------------
// URL discovery — paginate through the AdC news listing
// ---------------------------------------------------------------------------

/**
 * Discover article URLs from the AdC news/press releases listing.
 *
 * The listing at /pt/noticias-comunicados-e-intervencoes uses a "Carregar mais"
 * (Load More) button that appends ?page=N.  Each page shows 10 articles.
 * We paginate until no more articles are found or --max-pages is reached.
 */
async function discoverArticleUrls(): Promise<ListingEntry[]> {
  console.log("\nDiscovering article URLs from AdC news listing...");

  const entries: ListingEntry[] = [];
  const seenUrls = new Set<string>();
  let page = 0;
  let consecutiveEmpty = 0;

  while (page <= maxPages) {
    const pageUrl =
      page === 0
        ? `${BASE_URL}${LISTING_PATH}`
        : `${BASE_URL}${LISTING_PATH}?page=${page}`;

    console.log(`  Fetching listing page ${page}: ${pageUrl}`);
    const html = await rateLimitedFetch(pageUrl);

    if (!html) {
      console.warn(`  [WARN] Could not fetch page ${page}`);
      consecutiveEmpty++;
      if (consecutiveEmpty >= 3) {
        console.log("  Stopping after 3 consecutive empty pages.");
        break;
      }
      page++;
      continue;
    }

    const $ = cheerio.load(html);
    let pageEntries = 0;

    // Articles are listed as linked items with title, date badge, and type badge.
    // Each article link points to /pt/artigos/[slug].
    $('a[href*="/pt/artigos/"]').each((_i, el) => {
      const href = $(el).attr("href");
      if (!href) return;

      const fullUrl = href.startsWith("http") ? href : `${BASE_URL}${href}`;

      // Skip non-article links (anchors, fragments, etc.)
      if (!fullUrl.includes("/pt/artigos/")) return;

      // Deduplicate
      if (seenUrls.has(fullUrl)) return;
      seenUrls.add(fullUrl);

      // Extract title from the link text
      const title = $(el).text().trim();
      if (!title || title.length < 5) return;

      // Try to extract date from a nearby element or parent container
      const parentBlock = $(el).closest("div, article, li");
      const dateText = parentBlock.find("time").attr("datetime") ??
        parentBlock.text().match(/(\d{2})-(\d{2})-(\d{4})/)?.[0] ??
        null;

      // Try to extract type badge (Comunicados, Noticias, Intervencoes)
      const typeText = parentBlock.text().toLowerCase();
      let entryType = "noticias";
      if (typeText.includes("comunicado")) {
        entryType = "comunicados";
      } else if (typeText.includes("interven")) {
        entryType = "intervencoes";
      }

      entries.push({
        url: fullUrl,
        title,
        date: dateText ? parsePortugueseDate(dateText) : null,
        type: entryType,
      });
      pageEntries++;
    });

    console.log(`    Found ${pageEntries} articles on page ${page}`);

    if (pageEntries === 0) {
      consecutiveEmpty++;
      if (consecutiveEmpty >= 3) {
        console.log("  Stopping: no more articles found.");
        break;
      }
    } else {
      consecutiveEmpty = 0;
    }

    // Check if there is a next page link
    const hasNextPage =
      $(`a[href*="page=${page + 1}"]`).length > 0 ||
      $('a:contains("Carregar mais")').length > 0;

    if (!hasNextPage && pageEntries === 0) {
      break;
    }

    page++;
  }

  console.log(`  Total: ${entries.length} unique article URLs discovered`);
  return entries;
}

// ---------------------------------------------------------------------------
// Portuguese date parsing
// ---------------------------------------------------------------------------

const PORTUGUESE_MONTHS: Record<string, string> = {
  janeiro: "01",
  fevereiro: "02",
  marco: "03",
  março: "03",
  abril: "04",
  maio: "05",
  junho: "06",
  julho: "07",
  agosto: "08",
  setembro: "09",
  outubro: "10",
  novembro: "11",
  dezembro: "12",
};

/**
 * Parse a Portuguese date string to ISO format (YYYY-MM-DD).
 * Handles:
 *   - "12 de marco de 2026" / "2 de janeiro de 2025"
 *   - "dd-mm-yyyy" / "dd/mm/yyyy"
 *   - "yyyy-mm-dd" (already ISO)
 */
function parsePortugueseDate(raw: string): string | null {
  if (!raw) return null;
  const s = raw.trim().toLowerCase();

  // Try "d de mmmm de yyyy" (Portuguese textual date)
  const textMatch = s.match(
    /(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})/,
  );
  if (textMatch) {
    const [, day, monthName, year] = textMatch;
    const monthNum = PORTUGUESE_MONTHS[monthName!];
    if (monthNum) {
      return `${year}-${monthNum}-${day!.padStart(2, "0")}`;
    }
  }

  // Try dd-mm-yyyy or dd/mm/yyyy
  const dashMatch = s.match(/(\d{1,2})[-/](\d{1,2})[-/](\d{4})/);
  if (dashMatch) {
    const [, day, month, year] = dashMatch;
    return `${year}-${month!.padStart(2, "0")}-${day!.padStart(2, "0")}`;
  }

  // Try yyyy-mm-dd (already ISO)
  const isoMatch = s.match(/(\d{4})-(\d{2})-(\d{2})/);
  if (isoMatch) {
    return isoMatch[0];
  }

  return null;
}

// ---------------------------------------------------------------------------
// Page parsing — extract structured data from individual article pages
// ---------------------------------------------------------------------------

/**
 * Extract body text from an AdC article page.
 *
 * The AdC website (Drupal-based) uses standard article markup.
 * Content follows the h1 title within the #main-content area.
 */
function extractBodyText($: cheerio.CheerioAPI): string {
  // Remove navigation, headers, footers, and non-content elements
  $(
    "nav, footer, header, .breadcrumb, .menu, script, style, .cookie-banner, .share-buttons, .social-share",
  ).remove();

  const bodySelectors = [
    ".field--name-body",
    ".node__content .field--type-text-with-summary",
    "article .content",
    ".article-body",
    "#main-content article",
    "main article",
    "main .content",
  ];

  for (const sel of bodySelectors) {
    const el = $(sel);
    if (el.length > 0 && el.text().trim().length > 100) {
      return el.text().trim();
    }
  }

  // Fallback: collect paragraphs from main content area
  const paragraphs: string[] = [];
  $("#main-content p, main p, article p").each((_i, el) => {
    const text = $(el).text().trim();
    if (text.length > 30) paragraphs.push(text);
  });

  if (paragraphs.length > 0) {
    return paragraphs.join("\n\n");
  }

  // Last resort: grab remaining text after stripping nav elements
  return $("main, article, #main-content").text().trim();
}

/**
 * Extract a case number from article text.
 *
 * Patterns:
 *   - "PRC/2022/01" or "PRC/2022/1"
 *   - "Ccent. 73/2025" or "Ccent_2025_73"
 *   - "INQ/2022/1"
 *   - "processo n.o 73/2025"
 *   - Concentration number embedded in title: "91/2025"
 */
function extractCaseNumber(
  title: string,
  bodyText: string,
): string | null {
  const combined = `${title} ${bodyText.slice(0, 3000)}`;

  // PRC/YYYY/NN
  const prcMatch = combined.match(
    /PRC\s*[/\\]\s*(\d{4})\s*[/\\]\s*(\d{1,3})/i,
  );
  if (prcMatch) {
    return `PRC/${prcMatch[1]}/${prcMatch[2]!.padStart(2, "0")}`;
  }

  // Ccent. NN/YYYY or Ccent_YYYY_NN
  const ccentDotMatch = combined.match(
    /Ccent\.?\s*(\d{1,4})\s*[/\\]\s*(\d{4})/i,
  );
  if (ccentDotMatch) {
    return `Ccent/${ccentDotMatch[2]}/${ccentDotMatch[1]}`;
  }

  const ccentUnderMatch = combined.match(
    /Ccent_(\d{4})_(\d{1,4})/i,
  );
  if (ccentUnderMatch) {
    return `Ccent/${ccentUnderMatch[1]}/${ccentUnderMatch[2]}`;
  }

  // INQ/YYYY/NN
  const inqMatch = combined.match(
    /INQ\s*[/\\]\s*(\d{4})\s*[/\\]\s*(\d{1,3})/i,
  );
  if (inqMatch) {
    return `INQ/${inqMatch[1]}/${inqMatch[2]!.padStart(2, "0")}`;
  }

  // Concentration number in title: "operacao de concentracao NN/YYYY"
  const concMatch = combined.match(
    /(?:concentra[cç][aã]o|opera[cç][aã]o)\s+(\d{1,4})\s*[/\\]\s*(\d{4})/i,
  );
  if (concMatch) {
    return `Ccent/${concMatch[2]}/${concMatch[1]}`;
  }

  // Process number: "processo n.o NN/YYYY"
  const procMatch = combined.match(
    /processo\s+n\.?\s*[°ºo]?\s*(\d{1,4})\s*[/\\]\s*(\d{4})/i,
  );
  if (procMatch) {
    return `AdC/${procMatch[2]}/${procMatch[1]}`;
  }

  return null;
}

/**
 * Generate a fallback case number from the article URL slug.
 */
function generateCaseNumberFromUrl(url: string): string {
  const slug = url.split("/").pop() ?? "unknown";
  const cleaned = slug.slice(0, 80);
  return `AdC-WEB/${cleaned}`;
}

/**
 * Extract fine amounts from Portuguese competition text.
 * Handles: "4.519.000 euros", "278 mil euros", "38 milhoes de euros",
 * "EUR 3.092.000", "coima de 4,5 milhoes"
 */
function extractFineAmount(text: string): number | null {
  const lower = text.toLowerCase();

  const patterns: Array<{
    regex: RegExp;
    multiplier: (match: RegExpExecArray) => number;
  }> = [
    // "N milhoes de euros" / "N milhoes EUR"
    {
      regex:
        /([\d.,]+)\s*milh[oõ]es?\s+(?:de\s+)?(?:euros?|EUR)/gi,
      multiplier: (m) => parsePortugueseNumber(m[1]!) * 1_000_000,
    },
    // "N mil euros" / "N mil EUR"
    {
      regex:
        /([\d.,]+)\s*mil\s+(?:de\s+)?(?:euros?|EUR)/gi,
      multiplier: (m) => parsePortugueseNumber(m[1]!) * 1_000,
    },
    // Direct amounts: "EUR N" or "N euros" or "N EUR"
    // Pattern for amounts like "4.519.000 euros" or "EUR 3.092.000"
    {
      regex:
        /(?:EUR|€)\s*([\d.]+(?:,\d+)?)/gi,
      multiplier: (m) => parsePortugueseNumber(m[1]!),
    },
    {
      regex:
        /([\d.]+(?:,\d+)?)\s*(?:euros?|EUR|€)/gi,
      multiplier: (m) => parsePortugueseNumber(m[1]!),
    },
    // "coima de N" followed by multiplier
    {
      regex:
        /coima\s+(?:de\s+|no\s+(?:montante|valor)\s+(?:de\s+)?)?(?:EUR|€)?\s*([\d.,]+)\s*(?:milh[oõ]es?)?/gi,
      multiplier: (m) => {
        const raw = m[0]!.toLowerCase();
        const val = parsePortugueseNumber(m[1]!);
        if (raw.includes("milh")) return val * 1_000_000;
        return val;
      },
    },
  ];

  // Scan the text around fine-related keywords for better precision
  const fineContextPattern =
    /(?:coima|multa|san[cç][aã]o|penalidade|montante).{0,200}?(?:[\d.,]+)\s*(?:milh[oõ]es?|mil)?\s*(?:de\s+)?(?:euros?|EUR|€)/gi;

  const fineContexts = lower.match(fineContextPattern) ?? [lower];

  for (const context of fineContexts) {
    for (const { regex, multiplier } of patterns) {
      regex.lastIndex = 0;
      const match = regex.exec(context);
      if (match?.[1]) {
        const val = multiplier(match);
        if (!isNaN(val) && val > 0) return val;
      }
    }
  }

  return null;
}

/**
 * Parse a Portuguese-formatted number.
 * Portuguese uses dots as thousands separators and commas as decimal separators.
 * Examples: "4.519.000" -> 4519000, "3,5" -> 3.5, "278" -> 278
 */
function parsePortugueseNumber(raw: string): number {
  let cleaned = raw.trim();

  // If it has both dots and comma, dots are thousands separators
  if (cleaned.includes(".") && cleaned.includes(",")) {
    cleaned = cleaned.replace(/\./g, "").replace(",", ".");
  } else if (cleaned.includes(",")) {
    // Comma only: decimal separator
    cleaned = cleaned.replace(",", ".");
  } else if (cleaned.includes(".")) {
    // Dots only: check if it looks like thousands separators
    // e.g. "4.519.000" vs "3.5"
    const parts = cleaned.split(".");
    const allThreeDigitGroups = parts.slice(1).every((p) => p.length === 3);
    if (allThreeDigitGroups && parts.length > 1) {
      cleaned = cleaned.replace(/\./g, "");
    }
    // Otherwise leave as decimal (e.g. "3.5")
  }

  return parseFloat(cleaned);
}

/**
 * Extract cited legal articles (Lei da Concorrencia, TFUE) from text.
 */
function extractLegalArticles(text: string): string[] {
  const articles: string[] = [];
  const seen = new Set<string>();

  function addArticle(art: string): void {
    if (!seen.has(art)) {
      seen.add(art);
      articles.push(art);
    }
  }

  // Art. N.o or Artigo N.o of Lei da Concorrencia / Lei n.o 19/2012
  const leiConcPattern =
    /[Aa]rt(?:igo)?\.?\s*(\d+\.?[°ºo]?)\s*(?:(?:,?\s*n\.?\s*[°ºo]?\s*\d+\s*)?(?:(?:da|do)\s+)?)?(?:Lei\s+(?:da\s+)?Concorr[eê]ncia|Lei\s+n\.?\s*[°ºo]?\s*19\s*[/\\]\s*2012)/gi;
  let m: RegExpExecArray | null;

  while ((m = leiConcPattern.exec(text)) !== null) {
    addArticle(`Art. ${m[1]!.replace(/[°ºo]/g, ".°")} Lei n.° 19/2012`);
  }

  // Standalone article references with law number
  const standalonePattern =
    /[Aa]rt(?:igo)?\.?\s*(\d+\.?[°ºo]?)\s*(?:(?:,?\s*n\.?\s*[°ºo]?\s*\d+\s*)?)\s*(?:da|do)\s+(?:Lei|RJC|Decreto)/gi;
  while ((m = standalonePattern.exec(text)) !== null) {
    const articleRef = m[0]!.trim();
    if (articleRef.length < 100) {
      addArticle(articleRef);
    }
  }

  // Art. 101/102 TFUE / TFEU
  const euPattern =
    /[Aa]rt(?:igo)?\.?\s*(101|102)\s*[°ºo.]?\s*(?:do\s+)?(?:TFUE|TFEU|Tratado)/gi;
  while ((m = euPattern.exec(text)) !== null) {
    addArticle(`Art. ${m[1]}.° TFUE`);
  }

  return articles;
}

/**
 * Classify a decision based on its title, body text, and article type.
 */
function classifyDecisionType(
  title: string,
  bodyText: string,
  articleType: string,
  caseNumber: string,
): {
  isMerger: boolean;
  type: string | null;
  outcome: string | null;
  status: string;
} {
  const titleLower = title.toLowerCase();
  const allText = `${titleLower} ${bodyText.toLowerCase().slice(0, 5000)}`;

  // Merger classification
  const isMerger =
    caseNumber.startsWith("Ccent/") ||
    titleLower.includes("concentra") ||
    titleLower.includes("n\u00E3o oposi\u00E7\u00E3o") ||
    titleLower.includes("nao oposicao") ||
    titleLower.includes("aquisi\u00E7\u00E3o") ||
    titleLower.includes("aquisicao") ||
    titleLower.includes("controlo exclusivo") ||
    titleLower.includes("controlo conjunto") ||
    titleLower.includes("empresa comum") ||
    (titleLower.includes("notifica") &&
      (titleLower.includes("controlo") || titleLower.includes("fus")));

  // Decision type
  let type: string | null = null;
  if (
    allText.includes("cartel") ||
    allText.includes("acordo anticoncorrencial") ||
    allText.includes("fixa\u00E7\u00E3o de pre\u00E7os") ||
    allText.includes("fixacao de precos") ||
    allText.includes("reparti\u00E7\u00E3o de mercado") ||
    allText.includes("reparticao de mercado") ||
    allText.includes("pr\u00E1tica colusiva") ||
    allText.includes("pratica colusiva") ||
    allText.includes("pr\u00E1tica concertada") ||
    allText.includes("pratica concertada")
  ) {
    type = "cartel";
  } else if (
    allText.includes("abuso de posi\u00E7\u00E3o dominante") ||
    allText.includes("abuso de posicao dominante") ||
    allText.includes("posi\u00E7\u00E3o dominante") ||
    allText.includes("posicao dominante")
  ) {
    type = "abuse_of_dominance";
  } else if (
    allText.includes("inqu\u00E9rito setorial") ||
    allText.includes("inquerito setorial") ||
    allText.includes("estudo setorial") ||
    allText.includes("an\u00E1lise setorial") ||
    allText.includes("analise setorial") ||
    caseNumber.startsWith("INQ/")
  ) {
    type = "sector_inquiry";
  } else if (
    allText.includes("pr\u00E1tica restritiva") ||
    allText.includes("pratica restritiva") ||
    allText.includes("pr\u00E1ticas anticoncorrenciais") ||
    allText.includes("praticas anticoncorrenciais") ||
    allText.includes("no-poach") ||
    allText.includes("n\u00E3o aliciamento") ||
    allText.includes("nao aliciamento")
  ) {
    type = "restrictive_practice";
  } else if (isMerger) {
    type = "merger_control";
  } else if (
    articleType === "comunicados" &&
    (allText.includes("sanciona") || allText.includes("condena"))
  ) {
    type = "sanction";
  } else if (
    allText.includes("compromisso") ||
    allText.includes("cl\u00E1usula") ||
    allText.includes("clausula")
  ) {
    type = "commitment_decision";
  } else {
    type = "decision";
  }

  // Outcome classification
  let outcome: string | null = null;
  if (
    allText.includes("coima") ||
    allText.includes("multa") ||
    allText.includes("san\u00E7\u00E3o") ||
    allText.includes("sancao") ||
    allText.includes("sanciona") ||
    allText.includes("condena")
  ) {
    outcome = "fine";
  } else if (
    titleLower.includes("n\u00E3o oposi\u00E7\u00E3o") ||
    titleLower.includes("nao oposicao") ||
    allText.includes("decis\u00E3o de n\u00E3o oposi\u00E7\u00E3o") ||
    allText.includes("decisao de nao oposicao")
  ) {
    if (
      allText.includes("condi\u00E7\u00F5es") ||
      allText.includes("condicoes") ||
      allText.includes("compromisso") ||
      allText.includes("obriga\u00E7\u00E3o") ||
      allText.includes("obrigacao")
    ) {
      outcome = "cleared_with_conditions";
    } else if (allText.includes("fase 2") || allText.includes("fase ii")) {
      outcome = "cleared_phase2";
    } else {
      outcome = "cleared_phase1";
    }
  } else if (
    allText.includes("investiga\u00E7\u00E3o aprofundada") ||
    allText.includes("investigacao aprofundada") ||
    allText.includes("fase 2") ||
    allText.includes("fase ii")
  ) {
    outcome = "phase2_investigation";
  } else if (
    allText.includes("arquivamento") ||
    allText.includes("arquivado")
  ) {
    if (
      allText.includes("condi\u00E7\u00F5es") ||
      allText.includes("condicoes") ||
      allText.includes("compromisso")
    ) {
      outcome = "cleared_with_conditions";
    } else {
      outcome = "cleared";
    }
  } else if (
    allText.includes("proibida") ||
    allText.includes("proibido") ||
    allText.includes("oposi\u00E7\u00E3o") && allText.includes("fase 2")
  ) {
    outcome = "blocked";
  } else if (
    allText.includes("extin\u00E7\u00E3o") ||
    allText.includes("extincao")
  ) {
    outcome = "withdrawn";
  } else if (
    titleLower.includes("notifica") &&
    !titleLower.includes("decis\u00E3o") &&
    !titleLower.includes("decisao")
  ) {
    outcome = "pending";
  }

  // Status
  let status = "final";
  if (
    allText.includes("recurso") ||
    allText.includes("impugna\u00E7\u00E3o") ||
    allText.includes("impugnacao") ||
    allText.includes("tribunal") && allText.includes("recorr")
  ) {
    status = "appealed";
  } else if (
    allText.includes("em curso") ||
    allText.includes("investiga\u00E7\u00E3o em") ||
    allText.includes("investigacao em") ||
    outcome === "pending" ||
    outcome === "phase2_investigation"
  ) {
    status = "ongoing";
  }

  return { isMerger, type, outcome, status };
}

/**
 * Classify sector from Portuguese competition text.
 */
function classifySector(
  title: string,
  bodyText: string,
): string | null {
  const text = `${title} ${bodyText.slice(0, 3000)}`.toLowerCase();

  const sectorMapping: Array<{ id: string; patterns: string[] }> = [
    {
      id: "energy",
      patterns: [
        "energia",
        "eletricidade",
        "gas natural",
        "renov\u00E1vel",
        "renovavel",
        "e\u00F3lica",
        "eolica",
        "combust\u00EDvel",
        "combustivel",
        "petr\u00F3leo",
        "petroleo",
        "galp",
        "edp",
        "repsol",
      ],
    },
    {
      id: "banking",
      patterns: [
        "banc",
        "financeiro",
        "cr\u00E9dito",
        "credito",
        "seguro",
        "realkredit",
        "pagamento",
        "fintech",
        "unicre",
        "fidelidade",
        "novo banco",
        "caixa geral",
        "santander",
        "bpi",
      ],
    },
    {
      id: "telecommunications",
      patterns: [
        "telecomunica",
        "banda larga",
        "m\u00F3vel",
        "movel",
        "fibra",
        "televisa\u0303o",
        "televisao",
        "nos ",
        "meo ",
        "vodafone",
        "altice",
        "sport tv",
      ],
    },
    {
      id: "retail",
      patterns: [
        "retalho",
        "supermercado",
        "distribui\u00E7\u00E3o",
        "distribuicao",
        "alimentar",
        "consumidor",
        "continente",
        "pingo doce",
        "jer\u00F3nimo martins",
        "jeronimo martins",
        "sonae",
        "auchan",
        "lidl",
      ],
    },
    {
      id: "healthcare",
      patterns: [
        "sa\u00FAde",
        "saude",
        "hospital",
        "farmac",
        "medic",
        "cl\u00EDnica",
        "clinica",
        "luz sa\u00FAde",
        "cuf",
        "gases medicinais",
      ],
    },
    {
      id: "media",
      patterns: [
        "media",
        "comunica\u00E7\u00E3o social",
        "comunicacao social",
        "imprensa",
        "r\u00E1dio",
        "radio",
        "publicidade",
        "editorial",
      ],
    },
    {
      id: "transport",
      patterns: [
        "transporte",
        "ferrovia",
        "avia\u00E7\u00E3o",
        "aviacao",
        "mar\u00EDtimo",
        "maritimo",
        "porto",
        "log\u00EDstica",
        "logistica",
        "autoestrada",
        "brisa",
        "via verde",
        "tap",
      ],
    },
    {
      id: "construction",
      patterns: [
        "constru\u00E7\u00E3o",
        "construcao",
        "imobili\u00E1rio",
        "imobiliario",
        "cimento",
        "betao",
        "bet\u00E3o",
        "mota-engil",
        "engenharia civil",
      ],
    },
    {
      id: "technology",
      patterns: [
        "tecnologia",
        "digital",
        "software",
        "intelig\u00EAncia artificial",
        "inteligencia artificial",
        "plataforma online",
        "e-commerce",
        "dados",
      ],
    },
    {
      id: "agriculture",
      patterns: [
        "agr\u00EDcola",
        "agricola",
        "agroalimentar",
        "cereais",
        "lact",
        "vinhos",
        "pecu\u00E1ria",
        "pecuaria",
        "cerealis",
      ],
    },
    {
      id: "labour_market",
      patterns: [
        "mercado laboral",
        "mercado de trabalho",
        "trabalho tempor\u00E1rio",
        "trabalho temporario",
        "recursos humanos",
        "no-poach",
        "n\u00E3o aliciamento",
        "nao aliciamento",
        "emprego",
      ],
    },
    {
      id: "waste_management",
      patterns: [
        "res\u00EDduos",
        "residuos",
        "reciclagem",
        "ambiente",
        "saneamento",
        "urbaser",
        "fomentinvest",
      ],
    },
    {
      id: "automotive",
      patterns: [
        "autom\u00F3vel",
        "automovel",
        "ve\u00EDculo",
        "veiculo",
        "condu\u00E7\u00E3o",
        "conducao",
        "escola de condu",
      ],
    },
  ];

  for (const { id, patterns } of sectorMapping) {
    for (const p of patterns) {
      if (text.includes(p)) return id;
    }
  }

  return null;
}

/**
 * Extract merger parties from AdC article text.
 *
 * Common title patterns:
 *   - "decisao de nao oposicao na operacao de concentracao X / Y"
 *   - "X notifica a aquisicao do controlo exclusivo sobre Y"
 *   - "X e Y notificam a criacao de uma empresa comum"
 *   - "X / Y" in the title after concentration reference
 */
function extractMergerParties(
  title: string,
  bodyText: string,
): { acquiring: string | null; target: string | null } {
  // Pattern: "concentracao X / Y" or "concentracao NN/YYYY - X / Y"
  const concMatch = title.match(
    /concentra[cç][aã]o\s+(?:\d+\s*\/\s*\d+\s*[-–—]\s*)?(.+?)\s*[/\\*]\s*(.+?)$/i,
  );
  if (concMatch) {
    return {
      acquiring: cleanPartyName(concMatch[1]!),
      target: cleanPartyName(concMatch[2]!),
    };
  }

  // Pattern: "X notifica a aquisicao do controlo ... sobre Y"
  const notifMatch = title.match(
    /^(.+?)\s+notifica\s+.*?(?:sobre|de)\s+(.+?)$/i,
  );
  if (notifMatch) {
    return {
      acquiring: cleanPartyName(notifMatch[1]!),
      target: cleanPartyName(notifMatch[2]!),
    };
  }

  // Pattern: "X e Y notificam a criacao de uma empresa comum"
  const jvMatch = title.match(
    /^(.+?)\s+e\s+(?:a\s+)?(.+?)\s+notificam\s+.*empresa\s+comum/i,
  );
  if (jvMatch) {
    return {
      acquiring: cleanPartyName(jvMatch[1]!),
      target: cleanPartyName(jvMatch[2]!),
    };
  }

  // Fallback: look for "X / Y" pattern in title
  const slashMatch = title.match(/[-–—]\s*(.+?)\s*[/\\*]\s*(.+?)$/);
  if (slashMatch) {
    return {
      acquiring: cleanPartyName(slashMatch[1]!),
      target: cleanPartyName(slashMatch[2]!),
    };
  }

  // Try body text: "aquisicao ... X ... controlo ... Y"
  const bodyMatch = bodyText.match(
    /aquisi[cç][aã]o\s+(?:pel[oa]\s+)?(.{3,80}?)\s+(?:do|de)\s+controlo\s+(?:exclusivo\s+)?(?:sobre\s+)?(.{3,100}?)(?:\.|,)/i,
  );
  if (bodyMatch) {
    return {
      acquiring: cleanPartyName(bodyMatch[1]!),
      target: cleanPartyName(bodyMatch[2]!),
    };
  }

  return { acquiring: null, target: null };
}

function cleanPartyName(raw: string): string {
  return raw
    .replace(/^(?:AdC\s+adotou\s+.*?(?:de|na)\s+)/i, "")
    .replace(/\s+/g, " ")
    .replace(/^[-–—*\s]+/, "")
    .replace(/[-–—*\s]+$/, "")
    .trim()
    .slice(0, 200);
}

/**
 * Extract parties list for enforcement decisions.
 */
function extractDecisionParties(
  title: string,
  bodyText: string,
): string | null {
  const parties: string[] = [];
  const text = `${title} ${bodyText.slice(0, 3000)}`;

  // Look for "Visada(s):" or "Visado(s):" sections (common in AdC decisions)
  const visadaMatch = text.match(
    /[Vv]isad[oa]s?\s*[:\-]\s*(.{5,500}?)(?:\n|(?=[A-Z]{2}))/,
  );
  if (visadaMatch) {
    const names = visadaMatch[1]!
      .split(/[;,]|(?:\se\s)/)
      .map((n) => n.trim())
      .filter((n) => n.length > 2 && n.length < 150);
    parties.push(...names);
  }

  // Look for company names near "sanciona" / "condena"
  if (parties.length === 0) {
    const sanctionMatch = text.match(
      /(?:sanciona|condena)\s+(?:a\s+|o\s+)?(.{5,200}?)(?:\s+por\s+|\s+em\s+|\s+no\s+)/i,
    );
    if (sanctionMatch) {
      parties.push(sanctionMatch[1]!.trim());
    }
  }

  if (parties.length > 0) {
    return JSON.stringify(parties);
  }

  return null;
}

/**
 * Parse a single AdC article page.
 */
function parsePage(
  html: string,
  url: string,
  listingEntry: ListingEntry,
): { decision: ParsedDecision | null; merger: ParsedMerger | null } {
  const $ = cheerio.load(html);

  // Extract title
  const title =
    $("h1").first().text().trim() ||
    $('meta[property="og:title"]').attr("content")?.trim() ||
    listingEntry.title ||
    "";

  if (!title) {
    return { decision: null, merger: null };
  }

  // Extract body text
  const bodyText = extractBodyText($);

  if (!bodyText || bodyText.length < 50) {
    return { decision: null, merger: null };
  }

  // Extract date from page content
  const pageDateText =
    $("time").first().attr("datetime") ??
    $("time").first().text().trim() ??
    "";
  const pageDate =
    parsePortugueseDate(pageDateText) ?? listingEntry.date ?? null;

  // Case number: extract from text, fall back to URL
  const caseNumber =
    extractCaseNumber(title, bodyText) ??
    generateCaseNumberFromUrl(url);

  // Classify the article
  const { isMerger, type, outcome, status } = classifyDecisionType(
    title,
    bodyText,
    listingEntry.type,
    caseNumber,
  );

  // Sector
  const sector = classifySector(title, bodyText);

  // Summary: first 500 characters of body text, normalized
  const summary = bodyText.slice(0, 500).replace(/\s+/g, " ").trim();

  // Legal articles
  const legalArticles = extractLegalArticles(bodyText);

  if (isMerger) {
    const { acquiring, target } = extractMergerParties(title, bodyText);

    return {
      decision: null,
      merger: {
        case_number: caseNumber,
        title,
        date: pageDate,
        sector,
        acquiring_party: acquiring,
        target,
        summary,
        full_text: bodyText,
        outcome: outcome ?? "pending",
        turnover: null, // Not reliably available from HTML articles
      },
    };
  }

  // Non-merger decision
  const fineAmount = extractFineAmount(bodyText);
  const parties = extractDecisionParties(title, bodyText);

  return {
    decision: {
      case_number: caseNumber,
      title,
      date: pageDate,
      type,
      sector,
      parties,
      summary,
      full_text: bodyText,
      outcome: outcome ?? (fineAmount ? "fine" : "pending"),
      fine_amount: fineAmount,
      gwb_articles:
        legalArticles.length > 0 ? JSON.stringify(legalArticles) : null,
      status,
    },
    merger: null,
  };
}

// ---------------------------------------------------------------------------
// Sector definitions (Portuguese competition sectors)
// ---------------------------------------------------------------------------

const SECTOR_DEFINITIONS: Record<
  string,
  { name: string; name_en: string; description: string }
> = {
  energy: {
    name: "Energia",
    name_en: "Energy",
    description:
      "Eletricidade, gas natural, energias renovaveis, combustiveis, redes de distribuicao e comercializacao de energia em Portugal.",
  },
  banking: {
    name: "Banca e Servicos Financeiros",
    name_en: "Banking and Financial Services",
    description:
      "Bancos, seguradoras, servicos de pagamento, credito e infraestruturas de mercados financeiros em Portugal.",
  },
  telecommunications: {
    name: "Telecomunicacoes",
    name_en: "Telecommunications",
    description:
      "Comunicacoes moveis, banda larga, televisao por cabo, fibra otica e infraestrutura de telecomunicacoes em Portugal.",
  },
  retail: {
    name: "Comercio a Retalho",
    name_en: "Retail",
    description:
      "Comercio a retalho alimentar e nao alimentar, distribuicao e grande distribuicao em Portugal.",
  },
  healthcare: {
    name: "Saude",
    name_en: "Healthcare",
    description:
      "Hospitais privados, clinicas, industria farmaceutica, gases medicinais e equipamentos medicos em Portugal.",
  },
  media: {
    name: "Media e Comunicacao",
    name_en: "Media and Communications",
    description:
      "Imprensa, televisao, radio, plataformas digitais e agencias de publicidade em Portugal.",
  },
  transport: {
    name: "Transportes",
    name_en: "Transport",
    description:
      "Aviacao, transporte maritimo, ferrovia, autoestradas, logistica e transporte rodoviario em Portugal.",
  },
  construction: {
    name: "Construcao e Imobiliario",
    name_en: "Construction and Real Estate",
    description:
      "Construcao civil, materiais de construcao, cimento, promocao imobiliaria e engenharia em Portugal.",
  },
  technology: {
    name: "Tecnologia e Digital",
    name_en: "Technology and Digital",
    description:
      "Tecnologias de informacao, plataformas digitais, software, inteligencia artificial e comercio eletronico em Portugal.",
  },
  agriculture: {
    name: "Agricultura e Agroalimentar",
    name_en: "Agriculture and Agri-food",
    description:
      "Agricultura, producao agroalimentar, cereais, lacticinios e cadeia alimentar em Portugal.",
  },
  labour_market: {
    name: "Mercado de Trabalho",
    name_en: "Labour Market",
    description:
      "Praticas anticoncorrenciais no mercado laboral, incluindo acordos de nao aliciamento, trabalho temporario e recursos humanos.",
  },
  waste_management: {
    name: "Gestao de Residuos e Ambiente",
    name_en: "Waste Management and Environment",
    description:
      "Recolha e tratamento de residuos, reciclagem, saneamento e servicos ambientais em Portugal.",
  },
  automotive: {
    name: "Automovel e Mobilidade",
    name_en: "Automotive and Mobility",
    description:
      "Industria automovel, comercio de veiculos, escolas de conducao e servicos de mobilidade em Portugal.",
  },
};

// ---------------------------------------------------------------------------
// Database operations
// ---------------------------------------------------------------------------

function initDb(): Database.Database {
  const dir = dirname(DB_PATH);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
    console.log(`Created data directory: ${dir}`);
  }

  if (force && existsSync(DB_PATH)) {
    unlinkSync(DB_PATH);
    console.log("Deleted existing database (--force)");
  }

  const db = new Database(DB_PATH);
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");
  db.exec(SCHEMA_SQL);

  return db;
}

function prepareStatements(db: Database.Database) {
  const insertDecision = db.prepare(`
    INSERT OR IGNORE INTO decisions
      (case_number, title, date, type, sector, parties, summary, full_text, outcome, fine_amount, gwb_articles, status)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const upsertDecision = db.prepare(`
    INSERT INTO decisions
      (case_number, title, date, type, sector, parties, summary, full_text, outcome, fine_amount, gwb_articles, status)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(case_number) DO UPDATE SET
      title = excluded.title,
      date = excluded.date,
      type = excluded.type,
      sector = excluded.sector,
      parties = excluded.parties,
      summary = excluded.summary,
      full_text = excluded.full_text,
      outcome = excluded.outcome,
      fine_amount = excluded.fine_amount,
      gwb_articles = excluded.gwb_articles,
      status = excluded.status
  `);

  const insertMerger = db.prepare(`
    INSERT OR IGNORE INTO mergers
      (case_number, title, date, sector, acquiring_party, target, summary, full_text, outcome, turnover)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const upsertMerger = db.prepare(`
    INSERT INTO mergers
      (case_number, title, date, sector, acquiring_party, target, summary, full_text, outcome, turnover)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(case_number) DO UPDATE SET
      title = excluded.title,
      date = excluded.date,
      sector = excluded.sector,
      acquiring_party = excluded.acquiring_party,
      target = excluded.target,
      summary = excluded.summary,
      full_text = excluded.full_text,
      outcome = excluded.outcome,
      turnover = excluded.turnover
  `);

  const upsertSector = db.prepare(`
    INSERT INTO sectors (id, name, name_en, description, decision_count, merger_count)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
      decision_count = excluded.decision_count,
      merger_count = excluded.merger_count
  `);

  return {
    insertDecision,
    upsertDecision,
    insertMerger,
    upsertMerger,
    upsertSector,
  };
}

// ---------------------------------------------------------------------------
// Main ingestion pipeline
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  console.log("=== AdC (Autoridade da Concorrencia) Crawler ===");
  console.log(`  Database:   ${DB_PATH}`);
  console.log(`  Dry run:    ${dryRun}`);
  console.log(`  Resume:     ${resume}`);
  console.log(`  Force:      ${force}`);
  console.log(`  Max pages:  ${maxPages === Infinity ? "all" : maxPages}`);
  console.log("");

  // Load resume state
  const state = loadState();
  const processedSet = new Set(state.processedUrls);

  // Step 1: Discover article URLs from the listing pages
  const entries = await discoverArticleUrls();

  // Filter already-processed URLs (for --resume)
  const entriesToProcess = resume
    ? entries.filter((e) => !processedSet.has(e.url))
    : entries;

  console.log(`\nTotal articles discovered: ${entries.length}`);
  console.log(`Articles to process:      ${entriesToProcess.length}`);
  if (resume && entries.length !== entriesToProcess.length) {
    console.log(
      `  Skipping ${entries.length - entriesToProcess.length} already-processed URLs`,
    );
  }

  if (entriesToProcess.length === 0) {
    console.log("Nothing to process. Exiting.");
    return;
  }

  // Step 2: Initialize database (unless dry run)
  let db: Database.Database | null = null;
  let stmts: ReturnType<typeof prepareStatements> | null = null;

  if (!dryRun) {
    db = initDb();
    stmts = prepareStatements(db);
  }

  // Step 3: Process each article
  const initialDecisions = state.decisionsIngested;
  const initialMergers = state.mergersIngested;
  let decisionsIngested = state.decisionsIngested;
  let mergersIngested = state.mergersIngested;
  let errors = 0;
  let skipped = 0;

  const sectorCounts: SectorAccumulator = {};

  for (let i = 0; i < entriesToProcess.length; i++) {
    const entry = entriesToProcess[i]!;
    const progress = `[${i + 1}/${entriesToProcess.length}]`;

    console.log(`${progress} Fetching: ${entry.url}`);

    const html = await rateLimitedFetch(entry.url);
    if (!html) {
      console.log("  SKIP — could not fetch");
      state.errors.push(`fetch_failed: ${entry.url}`);
      errors++;
      continue;
    }

    try {
      const { decision, merger } = parsePage(html, entry.url, entry);

      if (decision) {
        if (dryRun) {
          console.log(
            `  DECISION: ${decision.case_number} — ${decision.title.slice(0, 80)}`,
          );
          console.log(
            `    type=${decision.type}, sector=${decision.sector}, outcome=${decision.outcome}, fine=${decision.fine_amount}`,
          );
        } else {
          const stmt = force ? stmts!.upsertDecision : stmts!.insertDecision;
          stmt.run(
            decision.case_number,
            decision.title,
            decision.date,
            decision.type,
            decision.sector,
            decision.parties,
            decision.summary,
            decision.full_text,
            decision.outcome,
            decision.fine_amount,
            decision.gwb_articles,
            decision.status,
          );
          console.log(`  INSERTED decision: ${decision.case_number}`);
        }

        decisionsIngested++;

        if (decision.sector) {
          if (!sectorCounts[decision.sector]) {
            sectorCounts[decision.sector] = {
              name: decision.sector,
              name_en: null,
              description: null,
              decisionCount: 0,
              mergerCount: 0,
            };
          }
          sectorCounts[decision.sector]!.decisionCount++;
        }
      } else if (merger) {
        if (dryRun) {
          console.log(
            `  MERGER: ${merger.case_number} — ${merger.title.slice(0, 80)}`,
          );
          console.log(
            `    sector=${merger.sector}, outcome=${merger.outcome}, acquiring=${merger.acquiring_party?.slice(0, 40)}`,
          );
        } else {
          const stmt = force ? stmts!.upsertMerger : stmts!.insertMerger;
          stmt.run(
            merger.case_number,
            merger.title,
            merger.date,
            merger.sector,
            merger.acquiring_party,
            merger.target,
            merger.summary,
            merger.full_text,
            merger.outcome,
            merger.turnover,
          );
          console.log(`  INSERTED merger: ${merger.case_number}`);
        }

        mergersIngested++;

        if (merger.sector) {
          if (!sectorCounts[merger.sector]) {
            sectorCounts[merger.sector] = {
              name: merger.sector,
              name_en: null,
              description: null,
              decisionCount: 0,
              mergerCount: 0,
            };
          }
          sectorCounts[merger.sector]!.mergerCount++;
        }
      } else {
        console.log(
          "  SKIP — could not parse (no title or insufficient text)",
        );
        skipped++;
      }

      // Save state periodically (every 25 URLs)
      state.processedUrls.push(entry.url);
      state.decisionsIngested = decisionsIngested;
      state.mergersIngested = mergersIngested;

      if ((i + 1) % 25 === 0) {
        saveState(state);
        console.log(`  [STATE] Saved progress at ${i + 1} URLs`);
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      console.error(`  [ERROR] Parsing failed for ${entry.url}: ${message}`);
      state.errors.push(`parse_error: ${entry.url} — ${message}`);
      errors++;
    }
  }

  // Step 4: Insert/update sectors
  if (!dryRun && db && stmts) {
    console.log("\nUpdating sector table...");
    const sectorTransaction = db.transaction(() => {
      for (const [id, counts] of Object.entries(sectorCounts)) {
        const def = SECTOR_DEFINITIONS[id];
        stmts!.upsertSector.run(
          id,
          def?.name ?? id,
          def?.name_en ?? null,
          def?.description ?? null,
          counts.decisionCount,
          counts.mergerCount,
        );
      }

      // Also insert any predefined sectors that had zero hits
      for (const [id, def] of Object.entries(SECTOR_DEFINITIONS)) {
        if (!sectorCounts[id]) {
          stmts!.upsertSector.run(
            id,
            def.name,
            def.name_en,
            def.description,
            0,
            0,
          );
        }
      }
    });
    sectorTransaction();
    console.log(
      `  Inserted/updated ${Object.keys(sectorCounts).length} active sectors + ${Object.keys(SECTOR_DEFINITIONS).length - Object.keys(sectorCounts).length} empty sectors`,
    );
  }

  // Step 5: Final state save
  saveState(state);

  // Step 6: Summary
  const decisionCount =
    !dryRun && db
      ? (
          db.prepare("SELECT count(*) as cnt FROM decisions").get() as {
            cnt: number;
          }
        ).cnt
      : decisionsIngested;

  const mergerCount =
    !dryRun && db
      ? (
          db.prepare("SELECT count(*) as cnt FROM mergers").get() as {
            cnt: number;
          }
        ).cnt
      : mergersIngested;

  const sectorCount =
    !dryRun && db
      ? (
          db.prepare("SELECT count(*) as cnt FROM sectors").get() as {
            cnt: number;
          }
        ).cnt
      : Object.keys(sectorCounts).length;

  console.log("\n=== Ingestion Complete ===");
  console.log(
    `  Decisions ingested (this run): ${decisionsIngested - initialDecisions}`,
  );
  console.log(
    `  Mergers ingested (this run):   ${mergersIngested - initialMergers}`,
  );
  console.log(`  Errors:                        ${errors}`);
  console.log(`  Skipped:                       ${skipped}`);
  console.log("");
  console.log("Database totals:");
  console.log(`  Decisions: ${decisionCount}`);
  console.log(`  Mergers:   ${mergerCount}`);
  console.log(`  Sectors:   ${sectorCount}`);
  console.log(`\nState saved to: ${STATE_FILE}`);

  if (state.errors.length > 0) {
    console.log(`\nErrors encountered (${state.errors.length}):`);
    for (const e of state.errors.slice(-20)) {
      console.log(`  - ${e}`);
    }
  }

  if (db) {
    db.close();
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
