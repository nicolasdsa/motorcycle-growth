# Data Catalog

This document defines the first data ingestion planning layer for the motorcycle growth project. The goal is to make explicit which official datasets are needed, what each one contributes to the research question, and how each source should enter the repository.

## Planning principles

- Do not assume undocumented APIs exist.
- Prefer official bulk files or official APIs when they are clearly exposed.
- Keep original files in `data/raw/` and postpone transformations to later ETL steps.
- Separate source-system metadata from downstream analytical tables.
- Treat uncertain access patterns as uncertain until a downloader is tested.

## Recommended raw-data layout

Use one folder per source system inside `data/raw/`:

```text
data/raw/
  senatran_motorcycle_fleet/
  ibge_population/
  sih_sus/
  sim_mortality/
  cnes_establishments/
  cnes_hospital_beds/
```

The CNES input was split into two raw folders because emergency-care capacity will likely depend on two different official assets:

- establishment-level records for fixed/mobile emergency services, including SAMU-related units when available in CNES
- hospital-bed records for ICU and other relevant inpatient capacity

## Dataset inventory

### 1. SENATRAN motorcycle fleet data

- Institution: Secretaria Nacional de Trânsito (SENATRAN), Ministério dos Transportes
- Purpose in the project: measure motorcycle fleet growth over time and build exposure denominators for health outcomes
- Unit of analysis: municipality by vehicle type by reference period
- Expected geographic key: municipality code or municipality name plus UF; code validation will be required during ETL
- Expected time key: month and year, or year depending on the official asset used
- Likely automation status: likely feasible
- Why the status is "likely feasible": SENATRAN exposes year pages for fleet statistics and also maintains a RENAVAM open-data area; however, the exact stable file endpoint to script against should be validated during downloader implementation
- Official pages:
  - https://www.gov.br/transportes/pt-br/assuntos/transito/conteudo-Senatran/estatisticas-frota-de-veiculos-senatran
  - https://dados.transportes.gov.br/dataset/registro-nacional-de-veiculos-automotores-renavam
- Files needed for a manual first pass:
  - municipality-level fleet tables containing motorcycles by period
  - if available separately, total fleet tables by municipality and type for the same periods
- Expected filename pattern: not fixed yet; preserve the original downloaded names
- Where they should come from: the official SENATRAN fleet statistics pages for each year or the RENAVAM open-data dataset resources
- Where to place them: `data/raw/senatran_motorcycle_fleet/`
- Important note: the exact vehicle categories to count as "motorcycle fleet" must be documented in ETL code later. This planning layer does not assume the final list of class labels.

### 2. IBGE population data

- Institution: Instituto Brasileiro de Geografia e Estatística (IBGE)
- Purpose in the project: provide population denominators for rates and standardization
- Unit of analysis: municipality by year
- Expected geographic key: IBGE municipality code
- Expected time key: year
- Likely automation status: likely feasible
- Why the status is "likely feasible": IBGE publishes official municipal population outputs and downloadable tables, but the exact asset chosen must match the study design, especially for intercensal years
- Official pages:
  - https://www.ibge.gov.br/estatisticas/sociais/populacao/9103-estimativas-de-populacao.html
  - https://www.ibge.gov.br/cidades-e-estados/
- Files needed for a manual first pass:
  - annual municipal population estimate tables for the full study period
  - if the project uses census anchor years, the official municipality-level census population tables as a separate input
- Expected filename pattern: preserve the original downloaded names because IBGE naming may vary across releases
- Where they should come from: the official IBGE population estimates page and, when necessary, municipality exports from Cidades@/SIDRA
- Where to place them: `data/raw/ibge_population/`
- Important note: this layer records the source only. The choice between census counts, annual estimates, or a combined denominator strategy remains an analytical decision for the next step.

### 3. SIH/SUS hospitalization data

- Institution: DATASUS, Ministério da Saúde
- Purpose in the project: measure hospitalizations associated with motorcycle-related trauma and other selected injury outcomes
- Unit of analysis: hospitalization record or monthly aggregated output, depending on the access method eventually adopted
- Expected geographic key: municipality code for residence and/or occurrence; the exact field used must be defined during ETL
- Expected time key: competence month or hospitalization date, depending on source extraction
- Likely automation status: likely feasible
- Why the status is "likely feasible": DATASUS exposes a public transfer flow for SIH/SUS. Automation should remain conservative and parameterized by competence and UF, without attempting broad or hidden extraction flows.
- Official pages:
  - https://datasus.saude.gov.br/acesso-a-informacao/producao-hospitalar-sih-sus/
  - https://datasus.saude.gov.br/informacoes-de-saude-tabnet/
- Files needed for a manual first pass:
  - official SIH/SUS exports covering all months and UFs in the study period
  - documentation or data-dictionary material for the fields needed to identify external causes, diagnoses, residence, occurrence, and competence
- Expected filename pattern: not fixed yet; do not rename raw files at download time
- Where they should come from: official DATASUS access pages, official tabulations, or official dissemination exports selected manually by the user
- Where to place them: `data/raw/sih_sus/`
- Important note: the motorcycle-trauma case definition must be treated as a separate transformation rule. It is not assumed in this planning layer.

### 4. SIM mortality data

- Institution: Ministério da Saúde / DATASUS / OpenDataSUS
- Purpose in the project: measure mortality associated with motorcycle-related causes and support mortality-rate calculations
- Unit of analysis: death record
- Expected geographic key: municipality code for residence and/or occurrence; the final choice depends on the analytical design
- Expected time key: year of death, and possibly month if used later
- Likely automation status: likely feasible
- Why the status is "likely feasible": OpenDataSUS lists the SIM dataset with machine-readable resources, but the exact downloader should still be validated against the current official resources before it is treated as production-ready
- Official pages:
  - https://dadosabertos.saude.gov.br/dataset
  - https://opendatasus.saude.gov.br/ne/dataset/groups/sim
  - https://datasus.saude.gov.br/estatisticas-vitais/
- Files needed for a manual first pass:
  - annual SIM files for all years in the study period
  - the official dictionary or metadata needed to identify cause-of-death fields and municipality fields
- Expected filename pattern: preserve the original official file names because the current resource naming was not validated in code here
- Where they should come from: the official OpenDataSUS SIM dataset resources or other official DATASUS mortality pages
- Where to place them: `data/raw/sim_mortality/`
- Important note: the future ETL will need an explicit rule for selecting motorcycle-related deaths from the official cause fields.

### 5. CNES establishments data

- Institution: Ministério da Saúde / OpenDataSUS / CNES
- Purpose in the project: identify emergency-care infrastructure, including establishment presence and potential SAMU-related service coverage
- Unit of analysis: health establishment record
- Expected geographic key: municipality code, CNES establishment identifier, and UF
- Expected time key: update date or reference period published by the source
- Likely automation status: likely feasible
- Why the status is "likely feasible": the official CNES dataset page exposes API and flat-file resources and appears to be regularly updated
- Official pages:
  - https://dadosabertos.saude.gov.br/dataset/cnes-cadastro-nacional-de-estabelecimentos-de-saude
  - https://opendatasus.saude.gov.br/ne/dataset/cnes-cadastro-nacional-de-estabelecimentos-de-saude
- Files needed for a manual first pass:
  - establishment files for the study period
  - supporting metadata or dictionary material needed to identify emergency establishments, mobile units, and service types
- Expected filename pattern: preserve original file names
- Where they should come from: the official CNES dataset resources or the official CNES API if later validated for stable extraction
- Where to place them: `data/raw/cnes_establishments/`
- Important note: SAMU is expected to be derived by filtering CNES data, not from a separate standalone dataset assumed in advance.

### 6. CNES hospital beds data

- Institution: Ministério da Saúde / OpenDataSUS / CNES
- Purpose in the project: measure emergency and critical-care capacity, especially ICU-related bed availability
- Unit of analysis: hospital-bed record or hospital-bed aggregate, depending on the official asset
- Expected geographic key: municipality code, establishment identifier, and UF
- Expected time key: reference year or update period, depending on the resource selected
- Likely automation status: likely feasible
- Why the status is "likely feasible": the official "Hospitais e Leitos" dataset exposes downloadable CSV resources and is updated on the official portal
- Official pages:
  - https://dadosabertos.saude.gov.br/dataset/hospitais-e-leitos
  - https://opendatasus.saude.gov.br/ne/dataset/hospitais-e-leitos
- Files needed for a manual first pass:
  - hospital and bed files covering the study period
  - metadata needed to identify ICU beds and other emergency-relevant bed categories
- Expected filename pattern: portal resources currently expose annual CSV zip files, but preserve the original file names in raw storage
- Where they should come from: the official "Hospitais e Leitos" dataset resources
- Where to place them: `data/raw/cnes_hospital_beds/`
- Important note: ICU definitions and bed-category filters should be documented in downstream transformation code, not assumed here.

## Automation snapshot

Likely feasible:

- SENATRAN motorcycle fleet
- IBGE population
- SIH/SUS hospitalization
- SIM mortality
- CNES establishments
- CNES hospital beds

## Raw-folder preparation checklist

1. Create or keep the source folders listed above under `data/raw/`.
2. Store only original files in those folders.
3. Do not merge, rename, or clean files before ETL code exists.
4. Keep any official dictionary, layout, or metadata documents in the same source folder when they are needed to interpret the files.
5. If a source was downloaded manually, record the download page URL and date in a future ingestion log.

## Next ETL-facing decision

Before implementing downloaders, define one source-by-source ingestion contract:

- preferred official access method
- expected raw file format
- minimum study period to support the first analysis
- target raw-to-interim normalization strategy
