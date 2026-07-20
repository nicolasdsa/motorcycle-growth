# Trilha

Plataforma educacional interativa para construir intuição e preparar respostas de entrevistas técnicas. O MVP entrega um fluxo completo de **load balancing** e um fallback pedagógico genérico para outros assuntos.

## O que já funciona

- criação por assunto, cargo, senioridade, profundidade, foco e idioma;
- classificação de domínio e abrangência;
- especificação declarativa completa validada por Pydantic;
- allowlists de visualizações/atividades e recusa de conteúdo executável;
- aula progressiva de load balancing com visualizações e laboratório;
- fallback genérico para assuntos ainda sem experiência especializada;
- biblioteca, pesquisa, favorito, exclusão e versão do conteúdo;
- progresso por seção, estado da atividade e respostas de prática;
- regeneração isolada de seção;
- rotas internas de autoria para o Codex importar especificações e imagens originais;
- tema claro/escuro, layout responsivo, teclado e movimento reduzido;
- PostgreSQL, migration Alembic e Docker Compose.

As decisões e o contrato editorial estão em [docs/architecture.md](docs/architecture.md).

## Executar com Docker

```bash
cp .env.example .env
docker compose up --build
```

Abra `http://localhost:5173`. A API e seu OpenAPI ficam em `http://localhost:8000/docs`. A migration é executada automaticamente antes do servidor.

Para encerrar:

```bash
docker compose down
```

Os dados permanecem no volume `postgres_data`. Use `docker compose down -v` somente quando quiser apagar deliberadamente o banco local.

## Desenvolvimento sem Docker

Backend (SQLite local por padrão):

```bash
cd backend
poetry install
poetry run alembic upgrade head
poetry run uvicorn app.main:app --reload
```

Frontend, em outro terminal:

```bash
cd frontend
npm install
npm run dev
```

Variáveis suportadas pelo backend:

| Variável | Padrão | Descrição |
|---|---|---|
| `DATABASE_URL` | `sqlite:///./trilha.db` | URL SQLAlchemy; Compose usa PostgreSQL |
| `CORS_ORIGINS` | `http://localhost:5173` | origens separadas por vírgula |
| `ENVIRONMENT` | `development` | nome do ambiente |
| `VITE_API_URL` | `/api` | base da API no build web |
| `AUTHORING_TOKEN` | vazio | se definido, exige `X-Authoring-Token` em `POST /api/authoring/*` |

## Autoria assistida por Codex

O produto não chama a conversa do Codex de dentro do navegador. Em vez disso, o Codex pode atuar como editor no ambiente de desenvolvimento: pesquisa fontes, gera uma especificação declarativa e a envia para a API.

```text
GET  /api/authoring/catalog
POST /api/authoring/assets
POST /api/authoring/lessons
GET  /api/assets/{asset_id}
```

`GET /api/authoring/catalog` retorna somente visualizações e atividades permitidas. `POST /api/authoring/assets` recebe JSON com `mime_type`, `alt_text` e `content_base64`; aceita PNG, JPEG, WebP ou GIF com até 5 MB. A resposta retorna um `asset_id` que pode ser usado em `visualization.asset_id` na especificação. `POST /api/authoring/lessons` recebe uma `LessonSpecification` 1.0 completa e só persiste depois da validação estrutural, semântica e das referências de assets.

Em desenvolvimento local, as rotas funcionam sem token. Em ambiente compartilhado, defina `AUTHORING_TOKEN` no backend e envie-o no header `X-Authoring-Token`. Não exponha essas rotas de autoria ao público sem autenticação e autorização adequadas.

## Testes e qualidade

```bash
cd backend
poetry run ruff check .
poetry run pytest

cd ../frontend
npm run lint
npm run test
npm run build
```

## Limites conscientes do MVP

O gerador atual é determinístico: isso deixa o fluxo confiável, offline e testável. A aula de load balancing é especializada; demais assuntos usam fallback genérico e deixam explícita a necessidade de fontes primárias específicas. A próxima evolução é adicionar pipeline de pesquisa e provedor de modelo atrás das portas já definidas, mantendo o mesmo schema e regeneração por seção.
