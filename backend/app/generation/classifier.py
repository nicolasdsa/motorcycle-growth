import re

from app.schemas.lesson import Archetype, Breadth, Domain


DOMAIN_KEYWORDS: list[tuple[Domain, tuple[str, ...]]] = [
    (Domain.DATABASES, ("acid", "sql", "banco", "database", "mvcc", "índice", "index", "transação")),
    (Domain.ALGORITHMS, ("algoritmo", "sort", "busca binária", "complexidade", "quick")),
    (Domain.DATA_STRUCTURES, ("hash", "árvore", "tree", "fila", "pilha", "lista ligada")),
    (Domain.DISTRIBUTED_SYSTEMS, ("load balancing", "balanceamento", "replicação", "sharding", "consenso")),
    (Domain.SOFTWARE_ARCHITECTURE, ("solid", "clean architecture", "hexagonal", "arquitetura")),
    (Domain.SECURITY, ("oauth", "jwt", "autenticação", "autorização", "segurança")),
    (Domain.NETWORKING, ("dns", "tcp", "http", "rede", "packet")),
    (Domain.CONCURRENCY, ("thread", "deadlock", "race condition", "concorrência", "paralelismo")),
    (Domain.DEVOPS, ("ci/cd", "docker", "kubernetes", "deploy", "devops")),
    (Domain.OBSERVABILITY, ("logs", "métricas", "traces", "observabilidade")),
    (Domain.FRONTEND, ("vue", "react", "css", "frontend", "event loop")),
    (Domain.BACKEND, ("fastapi", "backend", "api", "rest")),
]

BROAD_MARKERS = ("fundamentos", "introdução", "algoritmos", "estruturas de dados", "solid", "bancos de dados")
SPECIFIC_MARKERS = ("por que", "como ", "quando ", "diferença", "versus", " vs ", "?", "em uma ")


def classify_topic(topic: str) -> tuple[Domain, Breadth, list[Archetype]]:
    normalized = re.sub(r"\s+", " ", topic.strip().lower())
    domain = next(
        (candidate for candidate, words in DOMAIN_KEYWORDS if any(word in normalized for word in words)),
        Domain.GENERAL,
    )
    if any(marker in normalized for marker in SPECIFIC_MARKERS):
        breadth = Breadth.SPECIFIC
    elif any(marker == normalized or normalized.startswith(f"{marker} ") for marker in BROAD_MARKERS):
        breadth = Breadth.BROAD
    else:
        breadth = Breadth.FOCUSED

    archetypes = [Archetype.CONCEPT, Archetype.INTERVIEW]
    if domain in {Domain.DISTRIBUTED_SYSTEMS, Domain.SYSTEM_DESIGN, Domain.INFRASTRUCTURE}:
        archetypes.insert(1, Archetype.ARCHITECTURE)
    elif domain == Domain.ALGORITHMS:
        archetypes.insert(1, Archetype.ALGORITHM)
    elif domain == Domain.DATA_STRUCTURES:
        archetypes.insert(1, Archetype.DATA_STRUCTURE)
    elif domain == Domain.DATABASES:
        archetypes.insert(1, Archetype.DATABASE)
    elif domain in {Domain.SOFTWARE_ARCHITECTURE, Domain.SOFTWARE_ENGINEERING}:
        archetypes.insert(1, Archetype.REFACTORING)
    elif domain in {Domain.NETWORKING, Domain.SECURITY}:
        archetypes.insert(1, Archetype.PROTOCOL)
    return domain, breadth, archetypes

