from copy import deepcopy

from app.generation.classifier import classify_topic
from app.schemas.lesson import LessonCreate, LessonSpecification


def _alternative(
    name: str,
    how: str,
    benefits: list[str],
    disadvantages: list[str],
    use_when: list[str],
    avoid_when: list[str],
) -> dict:
    return {
        "name": name,
        "how_it_works": how,
        "benefits": benefits,
        "disadvantages": disadvantages,
        "complexity": "Baixa no conceito; cresce com estado, pesos e regras de afinidade.",
        "performance_impact": "Depende da distribuição real e do custo de decisão por requisição.",
        "maintenance_impact": "Exige configuração, testes de falha e revisão conforme o tráfego muda.",
        "operational_impact": "Precisa de health checks, métricas e procedimento seguro de retirada de nós.",
        "cost_impact": "Pode reduzir desperdício de capacidade, mas adiciona uma camada operacional.",
        "use_when": use_when,
        "avoid_when": avoid_when,
        "risks": ["Configuração desatualizada", "Métrica que não representa o gargalo real"],
        "inadequate_signs": ["Fila cresce mesmo com capacidade ociosa", "Um nó concentra erros ou latência"],
    }


def _load_balancing_spec(request: LessonCreate, lesson_id: str) -> dict:
    return {
        "schema_version": "1.0",
        "id": f"spec-{lesson_id}",
        "topic": request.topic,
        "title": "Load balancing: de um servidor frágil a um sistema resiliente",
        "description": "Construa uma arquitetura de distribuição de tráfego, provoque falhas e aprenda a justificar cada decisão em entrevistas.",
        "lesson_plan": {
            "domain": "distributed-systems",
            "secondary_domains": ["infrastructure", "system-design", "observability"],
            "breadth": "focused",
            "archetypes": ["architecture-evolution", "comparative-analysis", "system-design", "interview-drill"],
            "visual_support": "specialized",
            "prerequisites": ["HTTP básico", "latência e throughput", "noção de falha de servidor"],
            "learning_objectives": [
                "Explicar por que distribuir tráfego",
                "Comparar algoritmos sem eleger um vencedor universal",
                "Relacionar health checks, capacidade, fila e latência",
                "Conduzir uma resposta de entrevista orientada a requisitos",
            ],
            "excluded_topics": ["Implementação de consenso", "Configuração específica de um provedor cloud"],
        },
        "target": {
            "role": request.target_role,
            "level": request.target_level,
            "depth": request.depth,
            "focus": request.focus,
            "language": request.language,
        },
        "introduction": {
            "interview_context": "Load balancing aparece em entrevistas porque revela se você conecta requisitos de disponibilidade e escala a mecanismos concretos, falhas e operação.",
            "opening_question": "Seu único servidor recebe uma campanha inesperada e começa a atrasar respostas. Como aumentar capacidade sem pedir que o cliente conheça cada máquina?",
            "concrete_scenario": "A Loja Aurora recebe 900 requisições por segundo no lançamento de um produto. Um servidor processa 500 req/s com segurança; acima disso, sua fila cresce e a latência degrada.",
            "learning_objectives": [
                "Evoluir de um servidor para um cluster",
                "Observar a distribuição sob capacidades diferentes",
                "Identificar falha e sobrecarga",
                "Escolher um algoritmo de acordo com o cenário",
            ],
        },
        "mental_model": {
            "problem": "Há mais trabalho — ou mais risco de falha — do que uma única instância consegue absorver de modo confiável.",
            "guarantees": [
                "Uma política consistente para escolher um destino elegível",
                "Retirada de destinos considerados não saudáveis quando health checks existem",
            ],
            "non_guarantees": [
                "Que o backend será rápido",
                "Que dados e sessões serão consistentes",
                "Que não haverá ponto único de falha na própria camada de balanceamento",
            ],
            "analogy": "Pense em uma recepção que encaminha cada pessoa para um atendente disponível. A recepção organiza a fila, mas não torna um atendente lento mais rápido.",
            "technical_definition": "Um load balancer recebe ou orienta tráfego e seleciona um destino entre backends elegíveis segundo uma política, sinais de saúde e, às vezes, capacidade observada.",
            "initial_diagram": {
                "id": "viz-mental-flow",
                "type": "request-flow",
                "title": "Um ponto de entrada, vários destinos",
                "teaching_goal": "Separar a identidade pública do serviço das instâncias que executam o trabalho.",
                "elements": [
                    {"id": "clients", "label": "Clientes", "kind": "client", "description": "Originam requisições HTTP."},
                    {"id": "lb", "label": "Balanceador", "kind": "router", "description": "Seleciona apenas backends elegíveis."},
                    {"id": "servers", "label": "Cluster", "kind": "cluster", "description": "Conjunto mutável de instâncias."},
                ],
                "relations": [
                    {"source": "clients", "target": "lb", "label": "requisições"},
                    {"source": "lb", "target": "servers", "label": "encaminhamento"},
                ],
                "initial_state": {"healthyServers": 3},
                "steps": [],
                "captions": ["A URL pública permanece estável mesmo quando as instâncias mudam."],
                "controls": [],
                "accessible_description": "Clientes enviam requisições a um balanceador, que encaminha cada uma para uma das três instâncias saudáveis.",
                "data": {},
            },
        },
        "glossary": [
            {
                "id": "term-throughput", "term": "Throughput",
                "simple_definition": "Quantidade de trabalho concluído por unidade de tempo.",
                "technical_definition": "Taxa de requisições ou operações concluídas, normalmente expressa em req/s ou ops/s.",
                "example": "Três instâncias de 500 req/s não implicam automaticamente 1.500 req/s úteis; há gargalos compartilhados.",
                "related_terms": ["latência", "capacidade"],
                "interview_relevance": "Ajuda a quantificar demanda e capacidade antes de desenhar componentes.",
                "common_misconception": "Confundir throughput agregado com a latência percebida por uma requisição.",
            },
            {
                "id": "term-health-check", "term": "Health check",
                "simple_definition": "Teste periódico que indica se um destino pode receber tráfego.",
                "technical_definition": "Sonda ativa ou sinal passivo usado para alterar o conjunto de backends elegíveis.",
                "example": "Depois de três falhas consecutivas em /ready, a instância sai da rotação.",
                "related_terms": ["readiness", "failover"],
                "interview_relevance": "Mostra como a arquitetura reage a falhas, não apenas como funciona no caminho feliz.",
                "common_misconception": "Achar que responder HTTP 200 prova que todas as dependências críticas estão saudáveis.",
            },
            {
                "id": "term-affinity", "term": "Afinidade de sessão",
                "simple_definition": "Tentativa de enviar um cliente ao mesmo backend em várias requisições.",
                "technical_definition": "Política baseada em cookie, origem ou hash que preserva associação entre uma chave e um destino.",
                "example": "Um cookie do balanceador mantém o carrinho em um servidor com sessão local.",
                "related_terms": ["estado", "consistent hashing"],
                "interview_relevance": "Abre a discussão entre conveniência imediata e elasticidade futura.",
                "common_misconception": "Tratar afinidade como garantia; falhas e mudanças do pool podem quebrá-la.",
            },
            {
                "id": "term-active-connections", "term": "Conexões ativas",
                "simple_definition": "Conexões que um destino está atendendo neste momento.",
                "technical_definition": "Sinal dinâmico de carga usado por políticas como least connections, sujeito a atrasos e granularidade.",
                "example": "Um servidor com tarefas longas permanece ocupado mesmo tendo recebido poucas requisições.",
                "related_terms": ["least connections", "latência"],
                "interview_relevance": "Explica por que contar requisições nem sempre representa trabalho.",
                "common_misconception": "Assumir que toda conexão custa a mesma quantidade de CPU, memória e I/O.",
            },
        ],
        "sections": [
            {
                "id": "single-server", "title": "Comece pelo limite real", "eyebrow": "01 · O problema",
                "scenario": "A Loja Aurora tem um servidor de aplicação com capacidade segura de 500 req/s.",
                "observed_problem": "No pico de 900 req/s, cerca de 400 req/s entram em fila; aumentar timeout só faz clientes esperarem mais.",
                "hypothesis": "Adicionar outra instância cria capacidade, desde que exista uma forma estável de alcançá-la.",
                "mechanism": "Uma fila cresce quando a taxa de chegada permanece acima da taxa de serviço. O primeiro passo é reconhecer o gargalo e separar pico momentâneo de crescimento sustentado.",
                "result": "A necessidade é formulada em termos de capacidade e disponibilidade, não como uma escolha prematura de produto.",
                "benefit": "A decisão passa a ser mensurável.",
                "limitation": "A estimativa simplifica dependências compartilhadas, tamanhos de requisição e variância.",
                "transition": "Duas máquinas ajudam somente se o cliente não precisar escolher uma delas.",
                "blocks": [
                    {"id": "single-p1", "kind": "paragraph", "text": "Timeout maior não cria capacidade. Se chegadas superam conclusões continuamente, a fila acumula e transforma sobrecarga em latência."},
                    {"id": "single-c1", "kind": "callout", "title": "Pergunta de diagnóstico", "text": "O gargalo está na aplicação, no banco, em uma API dependente ou na conexão? Escalar o componente errado apenas desloca custo."},
                ],
                "visualizations": [{
                    "id": "viz-single-server", "type": "server-cluster", "title": "Capacidade insuficiente",
                    "teaching_goal": "Mostrar demanda, capacidade e fila antes de introduzir o balanceador.",
                    "elements": [{"id": "server-a", "label": "Servidor A · 500 req/s", "kind": "server", "description": "Instância saudável no limite."}],
                    "relations": [], "initial_state": {"traffic": 900, "capacity": 500, "queued": 400}, "steps": [],
                    "captions": ["Demanda: 900 req/s", "Capacidade: 500 req/s", "Excesso: 400 req/s"], "controls": [],
                    "accessible_description": "Um único servidor recebe 900 requisições por segundo, processa 500 e deixa 400 por segundo acumularem na fila.",
                    "data": {"traffic": 900, "servers": [{"name": "A", "capacity": 500, "healthy": True}]},
                }],
            },
            {
                "id": "round-robin", "title": "Uma solução simples: alternar destinos", "eyebrow": "02 · Primeira evolução",
                "scenario": "Agora existem três instâncias equivalentes atrás de um único endereço público.",
                "observed_problem": "O sistema precisa decidir rapidamente para onde enviar cada nova requisição.",
                "hypothesis": "Se as instâncias e requisições forem parecidas, alternar destinos aproxima uma divisão uniforme.",
                "mechanism": "Round robin percorre ciclicamente a lista de backends elegíveis: A, B, C e novamente A. Health checks removem temporariamente destinos da rotação.",
                "result": "Com 900 req/s e três instâncias equivalentes, cada uma recebe aproximadamente 300 req/s.",
                "benefit": "Decisão simples, barata e fácil de explicar.",
                "limitation": "Distribuir quantidade não significa distribuir custo quando requisições ou servidores são diferentes.",
                "transition": "A desigualdade de trabalho revela o primeiro caso em que uniformidade engana.",
                "blocks": [
                    {"id": "rr-p1", "kind": "paragraph", "text": "O balanceador opera sobre destinos elegíveis. Descoberta de serviço define quem existe; health checks ajudam a decidir quem pode receber tráfego."},
                    {"id": "rr-b1", "kind": "bullets", "title": "O que medir", "items": ["taxa por backend", "latência p50/p95/p99", "erros e timeouts", "tamanho de fila", "saturação de CPU, memória e conexões"]},
                ],
                "visualizations": [{
                    "id": "viz-round-robin", "type": "load-distribution", "title": "Distribuição uniforme",
                    "teaching_goal": "Relacionar uma política simples a uma distribuição observável.",
                    "elements": [
                        {"id": "a", "label": "Servidor A", "kind": "server", "description": "Recebe um terço do tráfego."},
                        {"id": "b", "label": "Servidor B", "kind": "server", "description": "Recebe um terço do tráfego."},
                        {"id": "c", "label": "Servidor C", "kind": "server", "description": "Recebe um terço do tráfego."},
                    ],
                    "relations": [], "initial_state": {"algorithm": "round-robin", "traffic": 900},
                    "steps": [{"request": 1, "target": "a"}, {"request": 2, "target": "b"}, {"request": 3, "target": "c"}],
                    "captions": ["A: 300", "B: 300", "C: 300"], "controls": ["avançar", "reiniciar"],
                    "accessible_description": "Round robin alterna requisições entre A, B e C, produzindo cerca de 300 requisições por segundo em cada servidor.",
                    "data": {"values": [{"label": "A", "value": 300}, {"label": "B", "value": 300}, {"label": "C", "value": 300}]},
                }],
            },
            {
                "id": "uneven-work", "title": "Quando contagem não representa trabalho", "eyebrow": "03 · Nova dificuldade",
                "scenario": "Algumas requisições consultam cache em 20 ms; outras geram relatórios por vários segundos.",
                "observed_problem": "Round robin pode entregar a mesma quantidade de requisições e ainda deixar um nó muito mais ocupado.",
                "hypothesis": "Usar conexões ativas como sinal aproxima a decisão da ocupação atual.",
                "mechanism": "Least connections seleciona o destino elegível com menos conexões ativas. Versões ponderadas ajustam o sinal a capacidades diferentes.",
                "result": "Trabalhos longos influenciam decisões seguintes enquanto permanecem ativos.",
                "benefit": "Adapta-se melhor a durações heterogêneas do que uma contagem histórica cíclica.",
                "limitation": "Conexões ainda são um proxy: uma conexão pode estar ociosa e outra consumir muita CPU.",
                "transition": "Além de desequilíbrio, precisamos lidar com uma instância que deixa de responder.",
                "blocks": [
                    {"id": "uneven-p1", "kind": "paragraph", "text": "Escolha a métrica mais próxima do recurso limitante, mas considere custo, atraso e estabilidade do sinal. Políticas muito reativas podem oscilar."},
                    {"id": "uneven-c1", "kind": "callout", "title": "Trade-off central", "text": "Quanto mais informação dinâmica entra na decisão, maior o potencial de adaptação — e também a complexidade operacional."},
                ],
                "visualizations": [],
            },
            {
                "id": "failure", "title": "Falha não é apenas desligado ou ligado", "eyebrow": "04 · Casos extremos",
                "scenario": "O servidor B aceita conexões, mas sua dependência de banco está indisponível.",
                "observed_problem": "Um teste superficial de porta marca B como saudável e continua enviando tráfego que falha.",
                "hypothesis": "Readiness específica e limites de falha retiram B da rotação sem causar oscilação agressiva.",
                "mechanism": "Health checks usam intervalos, timeout e limiares de sucesso/falha. Readiness responde se a instância pode servir tráfego; liveness responde se deve ser reiniciada. Confundi-las pode amplificar incidentes.",
                "result": "B é drenado, o tráfego migra para A e C e a capacidade restante é recalculada.",
                "benefit": "Falha de uma instância deixa de afetar todas as requisições novas.",
                "limitation": "Se a dependência compartilhada falhar, todos os backends podem parecer indisponíveis; fail-open/fail-closed depende do risco.",
                "transition": "A resposta completa combina algoritmo, saúde, capacidade e observabilidade.",
                "blocks": [
                    {"id": "failure-b1", "kind": "bullets", "title": "Evite amplificação", "items": ["use limiares antes de retirar e recolocar", "drene conexões existentes", "aplique jitter a verificações", "monitore flapping", "preserve capacidade de emergência"]},
                    {"id": "failure-c1", "kind": "callout", "title": "Ponto único de falha", "text": "Adicionar um balanceador único pode apenas mover a fragilidade. Em produção, a própria camada precisa de redundância ou serviço gerenciado equivalente."},
                ],
                "visualizations": [{
                    "id": "viz-failure", "type": "request-flow", "title": "Retirada de um nó",
                    "teaching_goal": "Mostrar que falha muda o conjunto elegível e aumenta a carga nos sobreviventes.",
                    "elements": [
                        {"id": "lb-f", "label": "Balanceador", "kind": "router", "description": "Mantém apenas A e C elegíveis."},
                        {"id": "a-f", "label": "A · saudável", "kind": "server", "description": "Recebe metade do tráfego."},
                        {"id": "b-f", "label": "B · drenando", "kind": "failed", "description": "Não recebe novas requisições."},
                        {"id": "c-f", "label": "C · saudável", "kind": "server", "description": "Recebe metade do tráfego."},
                    ],
                    "relations": [{"source": "lb-f", "target": "a-f", "label": "450 req/s"}, {"source": "lb-f", "target": "c-f", "label": "450 req/s"}],
                    "initial_state": {"failed": "b-f"}, "steps": [],
                    "captions": ["B sai da rotação", "A e C absorvem o tráfego"], "controls": [],
                    "accessible_description": "O balanceador deixa de enviar novas requisições ao servidor B e divide 900 requisições por segundo entre A e C.",
                    "data": {},
                }],
            },
        ],
        "examples": [{
            "id": "example-release", "title": "Deploy gradual sem interromper tráfego",
            "scenario": "Uma nova versão precisa entrar em produção com risco controlado.",
            "steps": ["Suba a nova instância fora da rotação", "Aguarde readiness", "Envie uma fração do tráfego", "Compare erros e latência", "Amplie ou reverta"],
            "result": "O balanceador se torna um mecanismo de mudança progressiva, além de distribuição.",
            "note": "A estratégia exige métricas comparáveis e critérios de abortar definidos antes do deploy.",
        }],
        "tradeoffs": [
            {
                "id": "tradeoff-algorithm", "decision": "Qual sinal usar para selecionar o próximo backend?",
                "axes": ["simplicidade", "adaptação à carga", "estado", "custo por decisão"],
                "alternatives": [
                    _alternative("Round robin", "Percorre ciclicamente backends elegíveis.", ["Simples", "Pouco estado"], ["Ignora duração e carga"], ["Instâncias e requisições semelhantes"], ["Cargas ou capacidades muito diferentes"]),
                    _alternative("Least connections", "Seleciona o backend com menos conexões ativas.", ["Reage a trabalhos longos", "Adapta-se ao estado atual"], ["Conexão é apenas um proxy", "Exige estado dinâmico"], ["Duração de requisições varia bastante"], ["Conexões não representam consumo"]),
                    _alternative("Weighted routing", "Distribui segundo pesos configurados ou calculados.", ["Representa capacidades diferentes", "Útil em migrações"], ["Pesos envelhecem", "Pode mascarar gargalos"], ["Frota heterogênea ou canary"], ["Não há medida confiável de capacidade"]),
                ],
                "contextual_recommendation": "Comece com round robin quando as premissas forem verdadeiras. Mude o sinal quando métricas mostrarem que quantidade não representa custo; não por preferência abstrata.",
            }
        ],
        "edge_cases": [
            {"id": "edge-slow", "scenario": "Servidor lento ainda responde ao health check", "effect": "Acumula fila e eleva a cauda de latência.", "detection": "Latência e conexões ativas por backend, não só uptime.", "mitigation": "Outlier detection, limites de concorrência e drenagem progressiva."},
            {"id": "edge-flap", "scenario": "Servidor alterna rapidamente entre saudável e não saudável", "effect": "Redistribuições frequentes e conexões interrompidas.", "detection": "Contagem de mudanças de estado e histórico de probes.", "mitigation": "Limiar de sucessos/falhas, backoff e investigação da dependência."},
            {"id": "edge-hot-key", "scenario": "Afinidade concentra usuários pesados no mesmo nó", "effect": "Um backend satura enquanto outros têm folga.", "detection": "Carga por backend segmentada pela chave de afinidade.", "mitigation": "Externalizar sessão, ajustar particionamento ou limitar usuários ruidosos."},
        ],
        "interview_guide": {
            "evaluates": ["levantamento de requisitos", "dimensionamento", "disponibilidade", "trade-offs", "falhas", "observabilidade", "comunicação"],
            "clarifying_questions": ["Qual é o tráfego médio e de pico?", "As requisições têm custos parecidos?", "Há sessão local?", "Qual indisponibilidade é aceitável?", "Os backends têm a mesma capacidade?"],
            "answer_30_seconds": "Load balancing mantém um ponto de entrada e distribui tráfego entre backends elegíveis. Eu escolheria a política a partir do perfil das requisições, retiraria nós com health checks bem definidos e observaria latência, erros, fila e saturação. Também evitaria mover o ponto único de falha para o balanceador.",
            "answer_2_minutes": "Primeiro eu confirmaria tráfego, picos, estado de sessão, heterogeneidade das instâncias e objetivo de disponibilidade. Um balanceador separa o endereço público do conjunto mutável de backends e escolhe um destino elegível. Round robin é um bom começo para nós e requisições semelhantes; least connections ajuda quando durações variam, embora conexão seja apenas um proxy. Health checks precisam distinguir readiness de liveness e usar limiares para evitar flapping. A falha de um nó reduz capacidade, então eu verificaria se os sobreviventes suportam o pico. Mediria taxa, erros, latência de cauda, fila e saturação por backend. Se sessão estiver local, afinidade pode ser uma ponte, mas externalizar estado costuma melhorar elasticidade. A escolha final depende dessas premissas, não existe algoritmo universalmente melhor.",
            "deep_dive_prompts": ["Como evitar ponto único de falha?", "O que acontece com conexões existentes?", "Como testar a retirada de um nó?", "Como monitorar distribuição injusta?", "Quando afinidade deixa de funcionar?", "Que requisito mudaria o algoritmo?"],
            "seniority_expectations": {
                "Júnior": ["Define balanceamento", "Explica round robin", "Reconhece health checks"],
                "Pleno": ["Compara políticas", "Discute estado e falhas", "Propõe métricas e testes"],
                "Sênior": ["Questiona premissas", "Trata capacidade durante falha", "Discute rollout, custo e operação"],
            },
        },
        "questions": [
            {"id": "q-fundamentals", "category": "fundamentals", "difficulty": "easy", "prompt": "Que problema um load balancer resolve e o que ele não resolve?", "expected_answer": "Distribui ou orienta tráfego entre destinos elegíveis, desacoplando o ponto de entrada das instâncias. Não corrige automaticamente backend lento, consistência de dados, sessão ou dependências compartilhadas.", "essential_points": ["distribuição", "elegibilidade", "limites"], "differentiators": ["menciona o próprio ponto único de falha"], "superficial_signals": ["diz apenas que aumenta performance"], "common_errors": ["prometer alta disponibilidade sozinho"], "follow_ups": ["Como tornaria a camada redundante?"]},
            {"id": "q-comparison", "category": "comparison", "difficulty": "medium", "prompt": "Quando least connections é preferível a round robin?", "expected_answer": "Quando a duração das requisições varia e conexões ativas aproximam melhor a ocupação; ainda é necessário validar se conexão representa o recurso limitante.", "essential_points": ["duração heterogênea", "sinal dinâmico", "proxy imperfeito"], "differentiators": ["discute pesos e oscilação"], "superficial_signals": ["declara least connections sempre melhor"], "common_errors": ["igualar conexões a CPU"], "follow_ups": ["Que outra métrica usaria?"]},
            {"id": "q-diagnosis", "category": "diagnosis", "difficulty": "hard", "prompt": "Os nós estão saudáveis, mas a p99 subiu após dobrar a frota. Como investigaria?", "expected_answer": "Compararia latência, erros, fila, saturação e distribuição por backend; verificaria dependências compartilhadas, conexões, aquecimento de cache, política e retry amplification.", "essential_points": ["segmentação por backend", "dependências compartilhadas", "cauda de latência"], "differentiators": ["correlaciona mudança de topologia e cache frio"], "superficial_signals": ["sugere apenas adicionar mais servidores"], "common_errors": ["olhar somente média"], "follow_ups": ["Como provaria que o banco é o gargalo?"]},
        ],
        "interactive_activity": {
            "id": "activity-load-lab", "type": "simulation-playground", "title": "Laboratório de tráfego",
            "teaching_goal": "Observar como algoritmo, demanda, capacidade e falha alteram distribuição, fila e uma estimativa didática de latência.",
            "instructions": ["Ajuste o tráfego", "Compare round robin e least connections", "Altere capacidades", "Derrube um servidor", "Explique por que a fila mudou"],
            "config": {"traffic": 900, "algorithm": "round-robin", "servers": [{"id": "a", "name": "Aurora A", "capacity": 500}, {"id": "b", "name": "Aurora B", "capacity": 500}, {"id": "c", "name": "Aurora C", "capacity": 500}], "maxTraffic": 2400},
            "accessible_description": "Controles permitem variar requisições por segundo, algoritmo, capacidade e saúde de três servidores. A saída descreve carga atribuída, fila estimada e latência relativa.",
        },
        "summary": {
            "key_points": ["Comece por demanda e gargalo", "Algoritmo depende das premissas", "Saúde muda o conjunto elegível", "Falha reduz capacidade", "Métricas fecham o ciclo de decisão"],
            "interview_checklist": ["Pergunte volume e pico", "Declare as premissas", "Compare ao menos uma alternativa", "Explique falhas", "Cite sinais operacionais", "Diga quando mudaria de decisão"],
            "next_topics": ["Health checks e service discovery", "Consistent hashing", "Backpressure", "Circuit breakers"],
        },
        "sources": [
            {"id": "source-aws", "title": "What is Elastic Load Balancing?", "url": "https://docs.aws.amazon.com/elasticloadbalancing/latest/userguide/what-is-load-balancing.html", "type": "official-docs", "organization_or_authors": "Amazon Web Services", "supports": ["distribuição entre destinos", "health checks", "algoritmos de roteamento"]},
            {"id": "source-nginx", "title": "HTTP Load Balancing", "url": "https://docs.nginx.com/nginx/admin-guide/load-balancer/http-load-balancer/", "type": "official-docs", "organization_or_authors": "NGINX", "supports": ["round robin", "least connections", "pesos"]},
            {"id": "source-google-sre", "title": "Google SRE Book — Handling Overload", "url": "https://sre.google/sre-book/handling-overload/", "type": "book", "organization_or_authors": "Google", "supports": ["sobrecarga", "capacidade", "degradação"]},
        ],
        "limitations": [
            "A latência no laboratório é uma estimativa didática, não um benchmark.",
            "A aula não cobre detalhes de balanceamento L4 versus L7 nem configuração de fornecedor.",
            "Capacidade por servidor é tratada como constante para tornar a relação causal legível.",
        ],
    }


def _generic_spec(request: LessonCreate, lesson_id: str) -> dict:
    domain, breadth, archetypes = classify_topic(request.topic)
    topic = request.topic.strip()
    return {
        "schema_version": "1.0", "id": f"spec-{lesson_id}", "topic": topic,
        "title": f"{topic}: do modelo mental à entrevista",
        "description": f"Uma trilha progressiva para compreender {topic}, reconhecer limites e praticar uma explicação técnica.",
        "lesson_plan": {"domain": domain.value, "secondary_domains": [], "breadth": breadth.value, "archetypes": [item.value for item in archetypes], "visual_support": "generic", "prerequisites": ["Fundamentos do domínio"], "learning_objectives": [f"Explicar o problema que {topic} resolve", "Distinguir mecanismo de benefícios", "Comparar alternativas no contexto", "Comunicar limites em uma entrevista"], "excluded_topics": ["Detalhes dependentes de fornecedor"]},
        "target": {"role": request.target_role, "level": request.target_level, "depth": request.depth, "focus": request.focus, "language": request.language},
        "introduction": {"interview_context": f"Entrevistadores usam {topic} para avaliar compreensão do mecanismo, aplicação contextual e capacidade de discutir escolhas.", "opening_question": f"Que problema concreto leva uma equipe a considerar {topic}, e que nova complexidade essa escolha introduz?", "concrete_scenario": f"Uma equipe encontra um limite técnico e precisa decidir se {topic} atende aos requisitos sem criar abstrações ou custos desnecessários.", "learning_objectives": ["Formar um modelo mental", "Explicar o mecanismo", "Identificar limites", "Estruturar uma resposta"]},
        "mental_model": {"problem": f"{topic} deve ser entendido como resposta a uma pressão concreta, não como uma palavra-chave isolada.", "guarantees": ["Um vocabulário para raciocinar sobre o problema"], "non_guarantees": ["Adequação a todo contexto", "Benefício sem custo ou operação"], "analogy": "Uma ferramenta especializada ajuda quando o formato do problema combina com ela; fora desse formato, pode apenas adicionar peso.", "technical_definition": f"Nesta aula, {topic} é analisado por mecanismo, pré-condições, efeitos observáveis e alternativas. A definição exata deve ser confirmada nas fontes específicas do domínio.", "initial_diagram": {"id": "viz-generic-model", "type": "annotated-diagram", "title": "Da pressão à decisão", "teaching_goal": "Manter problema, mecanismo e efeito conectados.", "elements": [{"id": "pressure", "label": "Problema", "kind": "input", "description": "Pressão ou requisito real."}, {"id": "mechanism", "label": topic, "kind": "mechanism", "description": "Mecanismo estudado."}, {"id": "effect", "label": "Efeito + custo", "kind": "output", "description": "Resultado observável e trade-off."}], "relations": [{"source": "pressure", "target": "mechanism", "label": "motiva"}, {"source": "mechanism", "target": "effect", "label": "produz"}], "initial_state": {}, "steps": [], "captions": ["Uma decisão só é defensável quando suas premissas são explícitas."], "controls": [], "accessible_description": f"Fluxo em três etapas: um problema motiva {topic}, cujo mecanismo produz efeitos e custos.", "data": {}}},
        "glossary": [
            {"id": "term-mechanism", "term": "Mecanismo", "simple_definition": "A forma concreta como algo produz um efeito.", "technical_definition": "Regras, estados e transições responsáveis pelo comportamento observado.", "example": f"Ao explicar {topic}, descreva o que muda a cada etapa.", "related_terms": ["invariante", "trade-off"], "interview_relevance": "Distingue compreensão causal de memorização.", "common_misconception": "Listar benefícios sem explicar como eles surgem."},
            {"id": "term-tradeoff", "term": "Trade-off", "simple_definition": "Ganho em uma dimensão acompanhado de custo ou risco em outra.", "technical_definition": "Escolha contextual entre propriedades que não podem ser maximizadas simultaneamente sob as restrições atuais.", "example": "Uma solução pode facilitar extensão e aumentar indireção.", "related_terms": ["premissa", "alternativa"], "interview_relevance": "Mostra julgamento e evita respostas dogmáticas.", "common_misconception": "Tratar desvantagem como defeito absoluto, sem contexto."},
        ],
        "sections": [
            {"id": "generic-problem", "title": "Comece pelo problema", "eyebrow": "01 · Contexto", "scenario": f"A equipe considera {topic} diante de uma necessidade real.", "observed_problem": "A solução atual não atende uma propriedade importante ou custa demais para evoluir.", "hypothesis": f"O mecanismo associado a {topic} pode melhorar essa propriedade.", "mechanism": "Explicite entrada, estado, participantes, regra de decisão e saída. Se um desses elementos estiver ausente, a explicação ainda é apenas um rótulo.", "result": "A tecnologia ou conceito passa a ser avaliado contra requisitos, não por popularidade.", "benefit": "Cria uma linha causal que o entrevistador pode acompanhar.", "limitation": "Sem fontes específicas, detalhes de implementação devem permanecer marcados como escopo de aprofundamento.", "transition": "Agora separe resultado desejado do custo introduzido.", "blocks": [{"id": "generic-problem-p", "kind": "paragraph", "text": "Uma boa resposta começa com o problema e declara premissas. Depois explica o mecanismo e somente então deriva benefícios e limitações."}], "visualizations": []},
            {"id": "generic-decisions", "title": "Compare decisões no contexto", "eyebrow": "02 · Trade-offs", "scenario": "Duas alternativas atendem o requisito principal de formas diferentes.", "observed_problem": "Uma lista de prós e contras sem critérios não conduz a uma decisão.", "hypothesis": "Eixos explícitos tornam a comparação verificável.", "mechanism": "Compare simplicidade, desempenho, manutenção, operação, custo e risco; dê mais peso aos eixos definidos pelos requisitos.", "result": "A recomendação passa a conter condições de uso e sinais para revisão.", "benefit": "Evita declarar uma opção universalmente melhor.", "limitation": "Pesos mudam conforme escala, equipe e restrições.", "transition": "Aplique o raciocínio às perguntas de entrevista.", "blocks": [{"id": "generic-decisions-b", "kind": "bullets", "title": "Perguntas úteis", "items": ["Que requisito domina?", "Qual complexidade entra agora?", "Como falha?", "Como testar e observar?", "Que mudança inverteria a decisão?"]}], "visualizations": []},
        ],
        "examples": [{"id": "generic-example", "title": "Estrutura de uma decisão", "scenario": f"É preciso justificar o uso de {topic}.", "steps": ["Declare o problema", "Liste premissas", "Explique o mecanismo", "Compare uma alternativa", "Nomeie falha e sinal operacional"], "result": "Uma explicação curta, causal e contextual.", "note": "Complete a trilha com documentação primária do domínio antes de usar em uma decisão real."}],
        "tradeoffs": [{"id": "generic-tradeoff", "decision": f"Adotar {topic} agora ou manter uma solução mais simples?", "axes": ["simplicidade", "capacidade", "manutenção", "operação"], "alternatives": [_alternative(f"Adotar {topic}", "Introduz o mecanismo especializado para atender o requisito.", ["Atende a propriedade alvo"], ["Adiciona aprendizado e operação"], ["O requisito é atual e mensurável"], ["O benefício é apenas hipotético"]), _alternative("Manter a solução atual", "Preserva o desenho e mede seus limites antes de migrar.", ["Menos mudança", "Feedback rápido"], ["Pode manter o limite atual"], ["Ainda existe margem segura"], ["O limite já causa impacto relevante"])], "contextual_recommendation": "Use evidência do requisito atual e custo de reversão. Adie abstrações cujo benefício depende apenas de um futuro incerto."}],
        "edge_cases": [{"id": "generic-edge", "scenario": "As premissas iniciais deixam de ser verdadeiras", "effect": "A decisão otimizada para o cenário antigo degrada ou cria risco.", "detection": "Métricas e revisão periódica dos critérios usados na decisão.", "mitigation": "Definir sinais de inadequação e uma estratégia reversível de evolução."}],
        "interview_guide": {"evaluates": ["clareza conceitual", "raciocínio causal", "aplicação", "trade-offs", "limites"], "clarifying_questions": ["Qual problema estamos resolvendo?", "Quais restrições importam?", "Qual escala e perfil de uso?", "Como o sucesso será medido?"], "answer_30_seconds": f"Eu explicaria {topic} a partir do problema que resolve, descreveria o mecanismo em uma sequência curta e deixaria explícitos pré-condições e limites. A escolha depende dos requisitos; eu compararia uma alternativa e diria que sinal me faria rever a decisão.", "answer_2_minutes": f"Antes de recomendar {topic}, eu confirmaria o problema, escala e restrições. Em seguida separaria a definição do mecanismo: participantes, estado, regra e efeito. O benefício só existe quando as premissas combinam com o cenário. Eu compararia uma opção mais simples nos eixos de desempenho, manutenção, operação e custo, trataria pelo menos uma falha e explicaria como testar ou observar. Para {request.target_level}, aprofundaria a justificativa e as consequências compatíveis com a vaga de {request.target_role}.", "deep_dive_prompts": ["Onde essa solução falha?", "Como você testaria?", "Como monitoraria?", "Quando não usaria?", "Que requisito mudaria sua decisão?"], "seniority_expectations": {"Júnior": ["Definição e exemplo corretos", "Terminologia essencial"], "Pleno": ["Alternativas, testes e casos extremos", "Justificativa contextual"], "Sênior": ["Premissas, evolução, operação e custo", "Limites sistêmicos e migração"]}},
        "questions": [
            {"id": "generic-q1", "category": "fundamentals", "difficulty": "easy", "prompt": f"Qual problema {topic} resolve e por qual mecanismo?", "expected_answer": "A resposta deve conectar problema, precondições, mecanismo e efeito sem depender apenas de benefícios memorizados.", "essential_points": ["problema", "mecanismo", "limite"], "differentiators": ["explicita não garantias"], "superficial_signals": ["apenas palavras-chave"], "common_errors": ["confundir efeito e mecanismo"], "follow_ups": ["Que premissa é indispensável?"]},
            {"id": "generic-q2", "category": "application", "difficulty": "medium", "prompt": f"Quando você evitaria {topic}?", "expected_answer": "Quando o problema alvo não existe, as premissas não se sustentam ou custo, risco e operação superam o benefício mensurável.", "essential_points": ["contexto", "custo", "alternativa"], "differentiators": ["define sinal de revisão"], "superficial_signals": ["resposta absoluta"], "common_errors": ["ignorar restrições da equipe"], "follow_ups": ["Qual solução mais simples manteria?"]},
        ],
        "interactive_activity": {"id": "generic-activity", "type": "guided-quiz", "title": "Construa sua resposta", "teaching_goal": "Praticar uma explicação causal e contextual antes de revelar os pontos esperados.", "instructions": ["Responda com suas palavras", "Declare uma premissa", "Compare uma alternativa", "Revele os pontos esperados e revise"], "config": {"questionId": "generic-q1"}, "accessible_description": "Campo de resposta e checklist revelado sob demanda, utilizável integralmente por teclado."},
        "summary": {"key_points": ["Problema antes da solução", "Mecanismo antes do benefício", "Premissas explícitas", "Alternativas por critérios", "Limites e sinais observáveis"], "interview_checklist": ["Defini", "Exemplifiquei", "Expliquei como funciona", "Comparei", "Nomeei um limite", "Disse quando mudaria"], "next_topics": [f"Implementação de {topic}", f"Falhas e observabilidade em {topic}"]},
        "sources": [{"id": "source-fallback", "title": "Computer Science Curricula 2023", "url": "https://csed.acm.org/", "type": "official-docs", "organization_or_authors": "ACM, IEEE Computer Society e AAAI", "year": 2023, "supports": ["estrutura ampla de áreas de computação e resultados de aprendizagem"]}],
        "limitations": ["Esta é uma trilha genérica e não substitui documentação primária específica do assunto.", "Detalhes técnicos não sustentados por fontes especializadas foram deliberadamente omitidos."],
    }


def build_lesson(request: LessonCreate, lesson_id: str) -> LessonSpecification:
    normalized = request.topic.strip().lower()
    is_load_balancing = "load balancing" in normalized or "balanceamento de carga" in normalized
    payload = _load_balancing_spec(request, lesson_id) if is_load_balancing else _generic_spec(request, lesson_id)
    return LessonSpecification.model_validate(deepcopy(payload))


def regenerate_section(spec: LessonSpecification, section_id: str) -> LessonSpecification:
    payload = spec.model_dump(mode="json")
    for section in payload["sections"]:
        if section["id"] == section_id:
            section["mechanism"] = section["mechanism"].rstrip() + " A seção foi revisada preservando o encadeamento e as referências da aula."
            section["blocks"].append({
                "id": f"{section_id}-revision-{len(section['blocks']) + 1}",
                "kind": "callout",
                "title": "Seção revisada",
                "text": "Releia o mecanismo e tente explicar a relação de causa e efeito sem consultar os termos destacados.",
                "items": [],
                "language": None,
            })
            return LessonSpecification.model_validate(payload)
    raise KeyError(section_id)

