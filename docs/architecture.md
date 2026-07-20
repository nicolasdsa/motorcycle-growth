# Arquitetura do MVP — Trilha

## 1. Visão geral

Trilha é uma aplicação web para construir intuição e preparar respostas de entrevistas técnicas. O MVP é um monólito modular: um frontend Vue conversa com uma API FastAPI, que persiste dados em PostgreSQL. O conteúdo gerado nunca é executado; ele é uma especificação JSON declarativa, validada antes da persistência e interpretada por renderizadores previamente registrados.

```text
Browser (Vue 3)
  ├─ formulário de criação
  ├─ biblioteca e progresso
  └─ renderizadores registrados + atividades registradas
              │ HTTP/JSON
FastAPI       ▼
  ├─ API / services / repositories
  ├─ classificação e geração
  ├─ validação estrutural e semântica
  └─ pesquisa/fontes (porta extensível)
              │ SQLAlchemy
PostgreSQL    ▼
  ├─ lessons (metadados + specification JSONB)
  ├─ progress
  └─ saved_answers
```

O primeiro fluxo vertical usa uma aula completa de load balancing. Assuntos não especializados recebem uma aula genérica coerente e podem ser substituídos por um provedor de IA no futuro sem alterar o contrato público.

## 2. Decisões técnicas

- **Monólito modular:** menor custo operacional e limites internos claros. Não há justificativa para microserviços no MVP.
- **Especificação declarativa:** a geração só produz dados. HTML, JavaScript, Vue, URLs `javascript:` e handlers inline são recusados.
- **Schema versionado (`1.0`):** permite migrar conteúdo sem acoplar a API ao layout atual.
- **PostgreSQL + JSONB:** metadados pesquisáveis ficam em colunas; a árvore editorial, naturalmente variável, fica em JSONB.
- **SQLite em testes/desenvolvimento opcional:** reduz fricção local, mantendo PostgreSQL como infraestrutura oficial.
- **Registros no frontend:** visualizações, atividades e seções desconhecidas caem em fallback textual acessível.
- **Sem Pinia no primeiro corte:** estado de servidor e de formulário é local; um store global acrescentaria indireção sem benefício.
- **Fontes seed verificáveis:** a aula inicial aponta para documentação pública real. A porta de pesquisa futura deverá normalizar e verificar fontes antes da geração.
- **Regeneração parcial:** a unidade de reparo é a seção. O serviço preserva todo o restante e incrementa a versão de conteúdo.

## 3. Modelo de dados

### `lessons`

| Campo | Tipo | Uso |
|---|---|---|
| `id` | UUID | identidade pública |
| `topic` | varchar(200) | busca e exibição |
| `slug` | varchar(240), unique | URL estável |
| `target_role` | varchar(32) | adaptação de entrevista |
| `target_level` | varchar(16) | Júnior/Pleno/Sênior |
| `depth` | varchar(16) | resumo/normal/aprofundada |
| `focus` | varchar(64), nullable | intenção opcional |
| `language` | varchar(16) | idioma do conteúdo |
| `is_favorite` | boolean | biblioteca |
| `content_version` | integer | controle editorial |
| `specification` | JSON/JSONB | `LessonSpecification` validada |
| `created_at`, `updated_at` | timestamptz | auditoria |

### `progress`

Um registro por aula no MVP sem autenticação. Contém `completed_section_ids`, percentual derivado, `activity_state`, `last_section_id` e timestamps.

### `saved_answers`

Respostas do usuário vinculadas à aula e ao `question_id`; a chave única `(lesson_id, question_id)` permite upsert.

## 4. Schema completo da aula

`LessonSpecification` possui:

- `schema_version`, `id`, `topic`, `title`, `description`;
- `lesson_plan`: domínio principal/secundários, abrangência, arquétipos, suporte visual, pré-requisitos, objetivos e exclusões;
- `target`: cargo, senioridade, profundidade, foco e idioma;
- `introduction`: contexto de entrevista, pergunta inicial, cenário e objetivos;
- `mental_model`: problema, garantias, não garantias, analogia, definição técnica e visualização inicial opcional;
- `glossary[]`: termo, definições simples/técnicas, exemplo, relações, relevância e equívoco comum;
- `sections[]`: id, título, etapa, cenário, problema, hipótese, mecanismo, resultado, benefício, limitação, transição, blocos e visualizações;
- `examples[]`: id, título, cenário, passos, resultado e observação;
- `tradeoffs[]`: decisão, alternativas, eixos, benefícios, desvantagens, complexidade, impactos, uso/evitação, riscos e sinais de inadequação;
- `edge_cases[]`: cenário, efeito, detecção e mitigação;
- `interview_guide`: avaliação, perguntas iniciais, respostas de 30 segundos/2 minutos, aprofundamentos e expectativa por senioridade;
- `questions[]`: categoria, dificuldade, enunciado, resposta esperada, pontos essenciais, diferenciais, sinais superficiais, erros e continuações;
- `interactive_activity`: um único tipo, objetivo, instruções, configuração e descrição acessível;
- `summary`: pontos-chave, checklist e próximos assuntos;
- `sources[]`: título, URL, tipo, autores/organização, ano opcional e afirmações sustentadas;
- `limitations[]`: limites pedagógicos ou de escopo.

Todos os objetos referenciáveis têm IDs únicos. `section-ref` e demais referências internas devem apontar para IDs existentes. Quantidades são limitadas no Pydantic para impedir respostas descontroladas.

## 5. Schemas Pydantic

Os contratos executáveis ficam em `backend/app/schemas/lesson.py`. São usados tanto nas entradas/saídas da API quanto para revalidar o JSON antes de salvar. Enums fecham domínios, arquétipos, tipos visuais e atividades. Strings têm limites; URLs são HTTP(S); dados livres aceitam apenas JSON puro.

## 6. Catálogo inicial de visualizações

O registro inicial implementa:

| Tipo | Renderizador inicial | Fallback |
|---|---|---|
| `request-flow` | fluxo cliente → balanceador → servidores | descrição textual |
| `server-cluster` | cartões de nós, saúde e carga | lista de elementos |
| `load-distribution` | barras proporcionais por servidor | tabela de métricas |
| `step-by-step` | sequência progressiva | passos numerados |
| `comparison-table` | tabela responsiva | cards empilhados |
| `decision-matrix` | tabela de critérios | lista por alternativa |
| `timeline` | trilha temporal | lista ordenada |
| `annotated-diagram` | nós e relações genéricas | descrição textual |
| `code-walkthrough` | código estático anotado | texto pré-formatado |
| `callout-example` | destaque contextual | bloco de texto |

O schema conhece o catálogo permitido mais amplo solicitado no briefing; o `visualSupport` só pode ser `specialized` se a aula usar ao menos um componente especializado disponível.

## 7. Catálogo inicial de atividades

- `simulation-playground`: implementado para load balancing, com algoritmo, tráfego, capacidade, falha, distribuição, fila e latência estimada.
- `guided-quiz`: fallback universal implementado.
- `interactive-stepper`: fallback para mecanismos sequenciais.

Os demais tipos pertencem ao enum/roadmap, mas só são aceitos com suporte visual compatível e renderizador registrado no backend. Não há sliders decorativos: cada controle do playground altera uma relação causal explicada.

## 8. Endpoints

```text
GET    /api/health
POST   /api/lessons
GET    /api/lessons?q=&favorite=
GET    /api/lessons/{id}
DELETE /api/lessons/{id}
PATCH  /api/lessons/{id}/favorite
POST   /api/lessons/{id}/sections/{section_id}/regenerate
GET    /api/lessons/{id}/progress
PUT    /api/lessons/{id}/progress
PUT    /api/lessons/{id}/answers/{question_id}
GET    /api/authoring/catalog
POST   /api/authoring/assets
POST   /api/authoring/lessons
GET    /api/assets/{asset_id}
```

As rotas de autoria existem para um agente editorial local — por exemplo, Codex — e aceitam somente schema e imagens originais permitidas. Quando `AUTHORING_TOKEN` está definido, elas exigem o header `X-Authoring-Token`. Assets usam PNG/JPEG/WebP/GIF com até 5 MB, descrição alternativa obrigatória e são armazenados como binário no PostgreSQL; uma especificação só pode referenciar `asset_id` existente.

## 9. Fluxo de geração

1. Normalizar assunto, cargo, nível, profundidade, foco e idioma.
2. Classificar abrangência, domínio e arquétipos por regras determinísticas.
3. Recuperar fontes normalizadas pela porta `ResearchProvider`.
4. Escolher o gerador: template especializado de load balancing ou fallback genérico no MVP; provedor LLM depois.
5. Validar o objeto com Pydantic.
6. Executar validações semânticas e de segurança.
7. Se uma seção falhar, solicitar/reconstruir somente aquela seção e revalidá-la.
8. Persistir especificação e fontes, retornando JSON tipado ao frontend.

## 10. Fluxo de validação

- parse e versão do schema;
- campos e limites Pydantic;
- IDs globalmente únicos e referências resolvidas;
- visualizações e atividade em allowlists registradas;
- coerência entre `visualSupport` e componentes usados;
- exatamente uma atividade principal;
- presença de trade-offs, guia de entrevista, perguntas, resumo e fontes;
- URL de fonte HTTP(S);
- busca recursiva por tags `<script>`, esquemas perigosos, handlers inline e chaves de execução (`html`, `javascript`, `eval`, `script`, `componentCode`);
- validações específicas da seção na regeneração parcial.

## 11. Estrutura de diretórios

```text
backend/
  app/{api,schemas,models,repositories,services,generation,research,validation}
  alembic/  tests/
frontend/
  src/{pages,components,lesson,visualizations,activities,composables,services,types,router,tests}
docs/
docker-compose.yml
README.md
```

## 12. Plano incremental

1. Fluxo completo load balancing e fallback genérico.
2. ACID/isolamento com transaction lab.
3. Quick Sort com execução passo a passo.
4. SOLID com desafio de refatoração.
5. Extrair padrões observados e estabilizar schema 1.x.
6. Integrar pesquisa e provedor de modelo com reparo de seção.
7. Adicionar autenticação e progresso por usuário somente quando necessário.

## 13. Riscos técnicos

- **Conteúdo plausível porém incorreto:** exigir fontes, rastrear afirmações e manter geração seed até existir pipeline de pesquisa verificável.
- **Schema amplo demais cedo:** implementar poucos renderizadores e fallback; evoluir por versões compatíveis.
- **JSONB difícil de consultar:** manter metadados essenciais em colunas e criar projeções apenas quando consultas reais surgirem.
- **Simulações enganosas:** rotular números como estimativas, documentar a fórmula e não tratá-los como benchmark.
- **Regeneração quebrar referências:** validar a seção isolada e depois a especificação inteira antes de commit.
- **Acessibilidade de diagramas:** descrição textual sempre presente; controles via teclado; estado nunca comunicado só por cor.
- **Dependência de IA/rede:** templates locais mantêm o produto demonstrável e testável.

## 14. Critérios de teste

- Backend: schemas válidos/inválidos, IDs/referências, allowlists, código proibido, services, CRUD, busca, favorito, progresso, respostas, regeneração parcial e falha do gerador.
- Frontend: formulário, loading/erro/vazio, registros/fallback, atividade, perguntas com resposta oculta, tema, navegação por teclado e persistência.
- E2E: criar aula → validar/renderizar → interagir → salvar progresso → recarregar/restaurar.
- Infra: migration sobe em banco vazio; healthcheck; builds reprodutíveis dos dois containers.
