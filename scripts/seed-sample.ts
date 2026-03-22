/**
 * Seed the AdC database with sample decisions and mergers for testing.
 *
 * Includes representative AdC (Autoridade da Concorrencia) decisions and
 * merger control cases in Portuguese so MCP tools can be tested.
 *
 * Usage:
 *   npx tsx scripts/seed-sample.ts
 *   npx tsx scripts/seed-sample.ts --force
 */

import Database from "better-sqlite3";
import { existsSync, mkdirSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";

const DB_PATH = process.env["ADC_DB_PATH"] ?? "data/adc.db";
const force = process.argv.includes("--force");

const dir = dirname(DB_PATH);
if (!existsSync(dir)) {
  mkdirSync(dir, { recursive: true });
}

if (force && existsSync(DB_PATH)) {
  unlinkSync(DB_PATH);
  console.log(`Deleted existing database at ${DB_PATH}`);
}

const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");
db.pragma("foreign_keys = ON");
db.exec(SCHEMA_SQL);

console.log(`Database initialised at ${DB_PATH}`);

interface SectorRow {
  id: string;
  name: string;
  name_en: string;
  description: string;
  decision_count: number;
  merger_count: number;
}

const sectors: SectorRow[] = [
  {
    id: "energy",
    name: "Energia",
    name_en: "Energy",
    description: "Eletricidade, gas natural, energias renovaveis, redes de distribuicao e comercializacao de energia em Portugal. A AdC tem sido ativa na supervisao do mercado liberalizado da energia.",
    decision_count: 2,
    merger_count: 1,
  },
  {
    id: "banking",
    name: "Banca e Servicos Financeiros",
    name_en: "Banking and Financial Services",
    description: "Bancos, seguradoras, servicos de pagamento e infraestruturas de mercados financeiros em Portugal.",
    decision_count: 1,
    merger_count: 1,
  },
  {
    id: "telecommunications",
    name: "Telecomunicacoes",
    name_en: "Telecommunications",
    description: "Comunicacoes moveis, banda larga, televisao por cabo e infraestrutura de telecomunicacoes em Portugal.",
    decision_count: 1,
    merger_count: 2,
  },
  {
    id: "retail",
    name: "Comercio a Retalho",
    name_en: "Retail",
    description: "Comercio a retalho alimentar e nao alimentar, distribuicao e grande distribuicao em Portugal.",
    decision_count: 1,
    merger_count: 1,
  },
  {
    id: "healthcare",
    name: "Saude",
    name_en: "Healthcare",
    description: "Hospitais privados, clinicas, farmaceuticas e equipamentos medicos em Portugal.",
    decision_count: 0,
    merger_count: 1,
  },
  {
    id: "media",
    name: "Media e Comunicacao",
    name_en: "Media and Communications",
    description: "Imprensa, televisao, radio, plataformas digitais e agencias de publicidade em Portugal.",
    decision_count: 1,
    merger_count: 0,
  },
];

const insertSector = db.prepare(
  "INSERT OR IGNORE INTO sectors (id, name, name_en, description, decision_count, merger_count) VALUES (?, ?, ?, ?, ?, ?)",
);
for (const s of sectors) {
  insertSector.run(s.id, s.name, s.name_en, s.description, s.decision_count, s.merger_count);
}
console.log(`Inserted ${sectors.length} sectors`);

interface DecisionRow {
  case_number: string;
  title: string;
  date: string;
  type: string;
  sector: string;
  parties: string;
  summary: string;
  full_text: string;
  outcome: string;
  fine_amount: number | null;
  gwb_articles: string;
  status: string;
}

const decisions: DecisionRow[] = [
  {
    case_number: "PRC/2022/1",
    title: "EDP — Praticas de Abuso de Posicao Dominante no Mercado de Eletricidade",
    date: "2022-07-15",
    type: "abuse_of_dominance",
    sector: "energy",
    parties: JSON.stringify(["EDP — Energias de Portugal, S.A.", "EDP Comercial — Comercializacao de Energia, S.A."]),
    summary: "A AdC condenou a EDP por praticas de abuso de posicao dominante no mercado retalhista de eletricidade. A EDP foi sancionada por dificultar a mudanca de comercializador pelos clientes finais e por praticas de fidelizacao abusivas.",
    full_text: "A AdC concluiu a investigacao sobre as praticas comerciais da EDP no mercado retalhista de eletricidade em Portugal. A EDP detinha, a data dos factos, uma posicao dominante no mercado de comercializacao de eletricidade em Portugal.\n\nPraticas investigadas:\n1. Obstaculizacao da mudanca de comercializador — a EDP adotou praticas que dificultavam a transicao dos clientes para comercializadores alternativos, nomeadamente atraves de procedimentos administrativos excessivamente complexos e demoras injustificadas no processamento de pedidos de mudanca.\n2. Programas de fidelizacao abusivos — a EDP implementou programas de descontos e beneficios condicionados a compromisos de permanencia excessivamente longos, sem justificacao economica proporcional.\n3. Acesso discriminatorio a dados de consumo — a EDP dificultou o acesso de comercializadores concorrentes a dados de consumo necessarios para apresentar propostas competitivas.\n\nA AdC considerou que estas praticas constituem abuso de posicao dominante nos termos do artigo 11.° da Lei da Concorrencia (Lei n.° 19/2012) e do artigo 102.° do TFUE.\n\nA coima aplicada foi calculada tendo em conta a gravidade e duracao das infraccoes e o volume de negocios da EDP.",
    outcome: "fine",
    fine_amount: 38_000_000,
    gwb_articles: JSON.stringify(["Art. 11.° Lei n.° 19/2012", "Art. 102.° TFUE"]),
    status: "appealed",
  },
  {
    case_number: "PRC/2021/3",
    title: "Bancos — Acordo sobre Comissoes Bancarias",
    date: "2021-09-22",
    type: "cartel",
    sector: "banking",
    parties: JSON.stringify(["Caixa Geral de Depositos", "Banco Comercial Portugues", "Banco Santander Totta", "Novo Banco", "Banco BPI"]),
    summary: "A AdC investigou um alegado acordo entre os principais bancos portugueses sobre comissoes aplicadas a operacoes bancarias de retalho. O processo resultou em compromisos dos bancos de alterar as suas politicas de comissionamento.",
    full_text: "A AdC investigou as praticas dos principais bancos portugueses em relacao ao estabelecimento de comissoes bancarias aplicadas a produtos e servicos de retalho. A investigacao centrou-se na possibilidade de os bancos terem coordenado as suas politicas de comissionamento, em violacao do artigo 9.° da Lei da Concorrencia (proibicao de praticas concertadas).\n\nContexto do mercado:\nO setor bancario portugues e caracterizado por um nivel de concentracao elevado, com os cinco maiores bancos a controlarem mais de 80% do mercado. Esta estrutura oligopolista facilita a coordenacao tacita ou explicita.\n\nPraticas investigadas:\n- Uniformizacao de comissoes em produtos especificos (transferencias, manutencao de conta)\n- Comunicacoes entre responsaveis de pricing dos bancos\n- Paralelismo de comportamentos no momento de introducao de novas comissoes\n\nResultado:\nOs bancos apresentaram compromisos que incluem maior transparencia na comunicacao de comissoes, prazo minimo de aviso antes de alteracoes, e facilitacao da comparacao de ofertas pelos consumidores. A AdC aceitou os compromisos e arquivou o processo sem aplicacao de coima.",
    outcome: "cleared_with_conditions",
    fine_amount: null,
    gwb_articles: JSON.stringify(["Art. 9.° Lei n.° 19/2012", "Art. 101.° TFUE"]),
    status: "final",
  },
  {
    case_number: "PRC/2023/2",
    title: "Distribuidores de Combustiveis — Praticas de Fixacao de Precos",
    date: "2023-05-10",
    type: "cartel",
    sector: "energy",
    parties: JSON.stringify(["Galp Energia, SGPS, S.A.", "BP Portugal — Comercio de Combustiveis e Lubrificantes, S.A.", "Repsol Portuguesa, S.A.", "Total Portugal, S.A."]),
    summary: "A AdC investigou praticas de fixacao de precos nos mercados de combustiveis em Portugal. A investigacao detectou troca de informacao comercialmente sensivel sobre precos e margens entre distribuidores.",
    full_text: "A AdC realizou operacoes de busca e apreensao nas instalacoes de principais distribuidores de combustiveis em Portugal no ambito de uma investigacao por suspeita de cartel.\n\nA investigacao centrou-se na possibilidade de os distribuidores de combustiveis terem trocado informacao comercialmente sensivel sobre estrategias de precos, margens e planos comerciais, facilitando a coordenacao de comportamentos.\n\nMercado em analise:\nO mercado de distribuicao de combustiveis e Portugal e caracterizado por poucos operadores integrados (producao/refinacao, distribuicao e venda a retalho), com niveis de concentracao elevados que podem facilitar comportamentos anticoncorrenciais.\n\nEstado do processo: A investigacao encontra-se em curso. A AdC emitiu nota de ilicitude aos visados e aguarda-se resposta das partes antes da decisao final.",
    outcome: "prohibited",
    fine_amount: null,
    gwb_articles: JSON.stringify(["Art. 9.° Lei n.° 19/2012"]),
    status: "appealed",
  },
  {
    case_number: "PRC/2020/5",
    title: "NOS/MEO — Praticas de Fidelizacao Abusivas em Telecomunicacoes",
    date: "2020-11-30",
    type: "abuse_of_dominance",
    sector: "telecommunications",
    parties: JSON.stringify(["NOS, SGPS, S.A.", "MEO — Servicos de Comunicacoes e Multimédia, S.A."]),
    summary: "A AdC investigou praticas de fidelizacao em contratos de telecomunicacoes de NOS e MEO. O processo resultou em compromisos dos operadores de reduzir periodos de fidelizacao e melhorar as condicoes de rescisao.",
    full_text: "A AdC investigou as praticas de fidelizacao nos contratos de servicos de telecomunicacoes, tendo em conta as posicoes significativas de mercado da NOS e da MEO.\n\nPraticas investigadas:\n- Periodos de fidelizacao excessivamente longos (ate 36 meses) em pacotes combinados (voz+dados+TV)\n- Penalizacoes de rescisao desproporcionadas calculadas de forma pouco transparente\n- Dificuldade em comparar ofertas entre operadores\n\nCompromisos aceites:\n- Reducao do periodo de fidelizacao maximo para 24 meses nos novos contratos\n- Melhoria da transparencia no calculo de penalizacoes de rescisao\n- Facilitacao da portabilidade numerica e mudanca de operador\n\nA AdC aceitou os compromisos e arquivou o processo sem aplicacao de coima, considerando que os compromisos eram suficientes para resolver as preocupacoes de concorrencia identificadas.",
    outcome: "cleared_with_conditions",
    fine_amount: null,
    gwb_articles: JSON.stringify(["Art. 11.° Lei n.° 19/2012"]),
    status: "final",
  },
  {
    case_number: "INQ/2022/1",
    title: "Inquérito Setorial — Comercio a Retalho Alimentar em Portugal",
    date: "2022-12-20",
    type: "sector_inquiry",
    sector: "retail",
    parties: JSON.stringify(["Sonae — SGPS, S.A. (Continente)", "Jeronimo Martins Portugal (Pingo Doce)", "Grupo Auchan (Jumbo)", "El Corte Inglés Portugal"]),
    summary: "A AdC publicou os resultados do seu inquerito setorial ao comercio a retalho alimentar em Portugal. O relatorio analisou as relacoes entre retalhistas e fornecedores e identificou praticas potencialmente preocupantes.",
    full_text: "A AdC realizou um inquerito setorial aprofundado ao comercio a retalho alimentar, com o objetivo de avaliar o funcionamento concorrencial do setor e identificar eventuais preocupacoes.\n\nPrincipais conclusoes do relatorio:\n1. Concentracao elevada — os quatro principais retalhistas controlam mais de 70% das vendas de productos alimentares\n2. Assimetria negocial — os grandes retalhistas exercem poder negocial significativo sobre os fornecedores, especialmente PMEs\n3. Praticas identificadas como potencialmente preocupantes:\n   - Condicoes retroativas que impoem custos aos fornecedores de forma unilateral\n   - Pedidos de contribuicoes para abertura de novas lojas\n   - Prazos de pagamento excessivamente longos\n   - Discriminacao entre fornecedores nacionais e internacionais\n\nRecomendacoes:\n- Adocao de um codigo de boas praticas setorial\n- Reforco do mecanismo de resolucao alternativa de litigios\n- Implementacao de requisitos de transparencia nas relacoes retalhista-fornecedor\n\nO inquerito nao resultou em procedimento contraordenacional, mas identificou areas onde podem ser necessarias reformas legislativas ou autoregulatoria.",
    outcome: "cleared",
    fine_amount: null,
    gwb_articles: JSON.stringify(["Art. 10.° Lei n.° 19/2012"]),
    status: "final",
  },
];

const insertDecision = db.prepare(`
  INSERT OR IGNORE INTO decisions
    (case_number, title, date, type, sector, parties, summary, full_text, outcome, fine_amount, gwb_articles, status)
  VALUES
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const insertDecisionsAll = db.transaction(() => {
  for (const d of decisions) {
    insertDecision.run(d.case_number, d.title, d.date, d.type, d.sector, d.parties, d.summary, d.full_text, d.outcome, d.fine_amount, d.gwb_articles, d.status);
  }
});
insertDecisionsAll();
console.log(`Inserted ${decisions.length} decisions`);

interface MergerRow {
  case_number: string;
  title: string;
  date: string;
  sector: string;
  acquiring_party: string;
  target: string;
  summary: string;
  full_text: string;
  outcome: string;
  turnover: number | null;
}

const mergers: MergerRow[] = [
  {
    case_number: "Ccent/2022/3",
    title: "NOS / Sport TV — Concentracao no Mercado de Conteudos Desportivos",
    date: "2022-04-08",
    sector: "telecommunications",
    acquiring_party: "NOS, SGPS, S.A.",
    target: "Sport TV Portugal, S.A. (participacao adicional)",
    summary: "A AdC autorizou a aquisicao de uma participacao adicional na Sport TV pela NOS, com condicoes destinadas a garantir o acesso nao discriminatorio de outros operadores de telecomunicacoes ao canal.",
    full_text: "A AdC analisou a aquisicao pela NOS de uma participacao adicional na Sport TV, o principal canal de conteudos desportivos premium em Portugal. A Sport TV e o canal que detem os direitos de transmissao das principais ligas de futebol portuguesas e internacionais.\n\nPreocupacoes de concorrencia identificadas:\n- O controlo da NOS sobre a Sport TV poderia ser usado para discriminar outros operadores de telecomunicacoes no acesso ao canal\n- A exclusao de conteudos premium poderia dificultar a concorrencia no mercado de televisao por assinatura\n- Risco de bundling exclusivo Sport TV + servicos NOS\n\nCondicoes impostas:\n- A NOS deve disponibilizar a Sport TV a outros operadores em termos nao discriminatorios (mesmos precos e condicoes)\n- Proibicao de exclusividade na distribuicao da Sport TV durante 5 anos\n- Obrigacao de separacao funcional entre a gestao da Sport TV e as atividades comerciais da NOS\n\nA concentracao foi autorizada com estas condicoes, que visam garantir a manutencao de um mercado de televisao por assinatura competitivo em Portugal.",
    outcome: "cleared_with_conditions",
    turnover: 1_600_000_000,
  },
  {
    case_number: "Ccent/2021/7",
    title: "Luz Saude / Hospital Particular do Algarve — Setor da Saude Privada",
    date: "2021-08-25",
    sector: "healthcare",
    acquiring_party: "Luz Saude, S.A.",
    target: "Hospital Particular do Algarve, S.A.",
    summary: "A AdC autorizou a aquisicao do Hospital Particular do Algarve pela Luz Saude na fase 1. A analise concluiu que as sobreposicoes geograficas eram limitadas e nao criavam posicoes dominantes em mercados locais de prestacao de servicos de saude.",
    full_text: "A AdC analisou a concentracao entre a Luz Saude (maior grupo hospitalar privado de Portugal) e o Hospital Particular do Algarve. A Luz Saude e detida pelo grupo internacional Fosun International.\n\nAnalise dos mercados relevantes:\nOs mercados de prestacao de servicos hospitalares privados sao tipicamente definidos com base geografica local ou regional, uma vez que os doentes raramente viajam grandes distancias para tratamentos de saude rotineiros.\n\nConstatacoes:\n- No Algarve, a Luz Saude nao tinha presenca hospitalar significativa antes da concentracao\n- O Hospital Particular do Algarve compete principalmente com o Hospital de Faro (publico) e clinicas privadas\n- A sobreposicao entre as atividades das partes era limitada\n- A concentracao nao criava nem refor cava uma posicao dominante em qualquer mercado relevante\n\nA concentracao foi autorizada na fase 1 sem condicoes.",
    outcome: "cleared_phase1",
    turnover: 850_000_000,
  },
  {
    case_number: "Ccent/2023/2",
    title: "EDP Renovaveis / Projetos de Energia Eolica Offshore — Expansao",
    date: "2023-09-12",
    sector: "energy",
    acquiring_party: "EDP Renovaveis, S.A.",
    target: "Portfolio de projetos eolicos offshore (varios ativos)",
    summary: "A AdC autorizou a aquisicao de portfolio de projetos de energia eolica offshore pela EDPR. Dada a natureza incipiente dos mercados de energia eolica offshore em Portugal, a operacao nao suscitou preocupacoes de concorrencia.",
    full_text: "A AdC analisou a aquisicao pela EDP Renovaveis (EDPR) de um portfolio de projetos de energia eolica offshore em diferentes fases de desenvolvimento.\n\nContexto:\nA energia eolica offshore e um setor emergente em Portugal, com o primeiro parque offshore comercial ainda em fase de licenciamento. A EDPR e o maior produtor de energias renovaveis em Portugal e um dos maiores a nivel mundial.\n\nAnalise:\n- O mercado de energia eolica offshore em Portugal e ainda nascente, sem capacidade operacional instalada relevante\n- Os projetos adquiridos encontram-se em fases iniciais de desenvolvimento, sujeitos a licenciamento\n- A EDPR ja detinha participacoes em varios projetos offshore em Portugal e a nivel europeu\n- A operacao nao criava sobreposicoes horizontais significativas num mercado maduro\n\nA concentracao foi autorizada na fase 1. A AdC notou que o mercado de energias renovaveis em Portugal apresenta dinamicas competitivas distintas dos mercados de energia convencional, com multiplos agentes a competir por licencas e leiloes governamentais.",
    outcome: "cleared_phase1",
    turnover: 2_100_000_000,
  },
];

const insertMerger = db.prepare(`
  INSERT OR IGNORE INTO mergers
    (case_number, title, date, sector, acquiring_party, target, summary, full_text, outcome, turnover)
  VALUES
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const insertMergersAll = db.transaction(() => {
  for (const m of mergers) {
    insertMerger.run(m.case_number, m.title, m.date, m.sector, m.acquiring_party, m.target, m.summary, m.full_text, m.outcome, m.turnover);
  }
});
insertMergersAll();
console.log(`Inserted ${mergers.length} mergers`);

const decisionCount = (db.prepare("SELECT count(*) as cnt FROM decisions").get() as { cnt: number }).cnt;
const mergerCount = (db.prepare("SELECT count(*) as cnt FROM mergers").get() as { cnt: number }).cnt;
const sectorCount = (db.prepare("SELECT count(*) as cnt FROM sectors").get() as { cnt: number }).cnt;

console.log(`\nDatabase summary:`);
console.log(`  Sectors:    ${sectorCount}`);
console.log(`  Decisions:  ${decisionCount}`);
console.log(`  Mergers:    ${mergerCount}`);
console.log(`\nDone. Database ready at ${DB_PATH}`);

db.close();
