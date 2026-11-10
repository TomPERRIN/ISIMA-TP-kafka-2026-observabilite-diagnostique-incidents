# Famille D — dérive de contrat (famille sémantique)

> Document de référence pour la **famille D** : les pannes où les messages
> circulent sans aucun incident d'infrastructure (topics sains, lag nul,
> PostgreSQL opérationnel) mais dont le **contenu** a dérivé du contrat attendu.
>
> - Simulateur : [`scripts/inject-semantics-drift.sh`](../../scripts/inject-semantics-drift.sh)
> - Moteur de diagnostic : [`scripts/lib/diagnose_engine.py`](../../scripts/lib/diagnose_engine.py) —
>   la règle D est **proposée** dans ce document, pas encore codée (chantier ouvert).

---

## 1. Définition et périmètre

Les familles A, B et C sont des pannes d'infrastructure : elles laissent une
trace dans les signaux du socle.

| Famille | Panne | Signal d'infrastructure |
|---|---|---|
| A `consumer_backlog` | retard de lecture | lag qui grossit |
| B `kafka_replication` | réplication dégradée | URP, brokers, contrôleur |
| C `persistence` | PostgreSQL indisponible/dégradé | services en erreur, `db_*` figés |
| **D `semantic_drift`** | **dérive de contrat** | **aucun — tout est au vert** |

La famille D est la seule famille **invisible pour un système d'observation qui
ne lit que des compteurs d'infrastructure** : les messages sont livrés, le lag
reste à zéro, PostgreSQL écrit normalement — seul le **sens** des données a
changé. La détecter impose de lire le **contenu** (les motifs de rejet), pas
les compteurs.

## 2. Le contrat de la chaîne

Contrat appliqué par `pix-validator` (`validation.py:11-45`), rendu au client
par `pix-decision-engine` (`decision.py:79-104`) :

| Champ | Contrat attendu | Motif de rejet si violé |
|---|---|---|
| **13 champs obligatoires** : `transaction_id`, `event_time`, `emitter/beneficiary_account_id`, `emitter/beneficiary_pix_client_id`, `emitter/beneficiary_tax_id`, `amount`, `currency`, `status`, `scenario_type`, `producer_id` | présence (`validation.py:12-26`) — `None` et `""` comptent aussi comme manquants (ligne 29) | `missing_<champ>` |
| `amount` | **nombre** > 0 via `float()` (`validation.py:33`) — une chaîne `"12,50"` lève une `ValueError` non gérée : **c'est le mécanisme exact du poison** (le validateur crashe au lieu de rejeter, puis relit) | `amount_must_be_positive` ; crash validateur si type invalide |
| `emitter_account_id` ≠ `beneficiary_account_id` | comptes différents (`validation.py:35`) | `accounts_must_differ` |
| `status` | égal à `"RECEIVED"` exactement (`validation.py:37`) | `invalid_initial_status` |
| `*_tax_id` ↔ `*_pix_client_id` | cohérence contre le **fichier de référence clients** (`load_reference_clients`, `validation.py:5-8`) — un `tax_id` inconnu déclenche aussi le mismatch | `emitter_tax_pix_mismatch`, `beneficiary_tax_pix_mismatch` |
| décision rendue dans le SLA métier | horloge du decision-engine | `processing_timeout` (technique, hors D) |

`trace_id` et `decision_sla_seconds` sont présents dans les payloads mais
**non validés** (optionnels, absents de `required_fields`).

Sémantique de bout en bout : producteur `at_least_once` (acks `all`, retries 1)
+ consommation avec commit après traitement (`consumer_commit_strategy: after`,
`commit_batch_size: 1`) → **doublons possibles** ; l'idempotence repose sur
l'empreinte `source_transaction_id:retry_attempt:rejection_reason`
(`event_builder.py:39`, `persister-rejected/main.py:146`).

## 3. Les 3 constats de code (vérifiés dans le dépôt)

### C1 — le socle ne vérifie AUCUNE version de contrat

Aucun champ `schema_version` dans les messages, aucune vérification de format :
un client qui n'a pas mis à jour son application (`producer_id: old-client-v1`
du simulateur) est traité comme un message métier normal — ou pire, **boucle**
dessus (mode `poison`).

### C2 — toute erreur de validation est rendue au client comme un rejet bancaire générique

`decision.py:79` (`map_rejection_reason`) :

- `missing_*` → `BANK_REJECTED` « données obligatoires manquantes » (`decision.py:95-101`) ;
- motifs métier connus (`amount_must_be_positive`, `invalid_initial_status`,
  `*_tax_pix_mismatch`, `accounts_must_differ`) → `BANK_REJECTED` (`decision.py:87-93`) ;
- **motif inconnu → `TECHNICAL_FAILURE` « incident technique »** (`decision.py:104`).

Conséquence : le client ne peut **jamais** distinguer une dérive de contrat
d'un refus bancaire ou d'un incident technique — un nouveau mode d'échec de
contrat sort en « rejeté par la banque » ou en « incident technique », jamais
en « contrat ».

### C3 — le moteur de diagnostic n'a ni famille D ni lecture des motifs

- `diagnose_engine.py:97` :
  `VERDICTS = ("healthy", "consumer_backlog", "kafka_replication", "persistence", "indeterminate")`
  — 3 familles + indeterminate, **pas de famille D** ;
- `rejection_reason` : **aucune occurrence** dans `diagnose_engine.py` — le
  moteur ne lit jamais les motifs de rejet ;
- en cas de poison, le lag monte et le moteur conclut
  `consumer_backlog` (famille A) au lieu de contrat (famille D) — faux verdict.

## 4. Les 6 modes d'échec

| # | Mode | Injection (simulateur) | Comportement du socle | Verdict moteur actuel |
|---|---|---|---|---|
| 1 | `missing` | champ obligatoire retiré (client v1 : le champ n'existait pas encore) | rejet `missing_emitter_pix_client_id` ; client voit un rejet bancaire | aucun (plateforme au vert) |
| 2 | `enum` | `status = "PENDING_V2"` (enum ajouté côté serveur) | rejet `invalid_initial_status` | aucun |
| 3 | `extra` | champs nouveaux (`client_version`, `legacy_routing_hint`) | champ **recopié silencieusement** — la dérive sémantique pure, sans alerte | aucun |
| 4 | `malformed` | payload non-JSON | erreur de décodage : poison ou rejet selon l'endroit du décodage | A (si boucle) |
| 5 | `poison` | type changé (`amount = "12,50"` → `ValueError` dans `float()`) | le validateur **relit indéfiniment** (commit après traitement), aucun rejet produit | **A (faux)** — `consumer_backlog` au lieu de D |
| 6 | doublon | relivraison `at_least_once` (inhérente au socle) | doublons dédupliqués par empreinte | aucun (attendu, non incident) |

Les 5 premiers modes sont jouables :

```sh
./scripts/inject-semantics-drift.sh <missing|enum|extra|malformed|poison> [nombre]
# modes poison/malformed : terminer impérativement par
./scripts/inject-semantics-drift.sh cleanup
```

## 5. Règle D proposée (pour `diagnose_engine.py`)

Objectif : une 4e famille `semantic_drift` dans `VERDICTS`, avec 2 règles de
détection, **sans nouvelle sonde** — tout est déjà exposé par `/health/details`
(`details.postgres.top_rejection_reasons`, `services.pix-validator`, compteurs
et lag). Intégration : Tom (les garde-fous du moteur ne changent pas).

**Règle D1 — dérive visible (motif contractuel nouveau)**
- Déclencheur : `details.postgres.top_rejection_reasons` contient un motif
  **hors catalogue connu** (`amount_must_be_positive`, `accounts_must_differ`,
  `invalid_initial_status`, `emitter_tax_pix_mismatch`,
  `beneficiary_tax_pix_mismatch`, `processing_timeout`, `missing_*` déjà
  observés), alors que les signaux d'infra sont au vert (lag ≤ seuil, URP = 0,
  postgres ok, aucun service en erreur).
- Verdict : `semantic_drift` / cause `contract_drift` ; `evidence[]` taguée
  `postgres` + `platform` (double lecture conservée).
- Prochains contrôles : `tp-db.sh` (motifs par différence), reproduire avec le
  simulateur, identifier le producteur (`producer_id`).

**Règle D2 — poison (distinguer de A)**
- Déclencheur : `services.pix-validator` en erreur + lag du groupe validator
  qui **s'accroît** (≥ 2 échantillons espacés ≥ 10 s — même garde-fou anti
  « lag éclair » que le moteur) + `db_rejected_count` **figé** (aucun nouveau
  rejet produit).
- Verdict : `semantic_drift` / cause `contract_poison` — **pas**
  `consumer_backlog`.
- Discriminateur vs A : en A classique le validator reste `running` et le lag
  se draine (des rejets TTL sont produits) ; en poison il est en erreur et
  **rien** n'est produit.
- Action : `./scripts/inject-semantics-drift.sh cleanup` (cf.
  [`pix-validation-stop-protocol.md`](pix-validation-stop-protocol.md)).

**Ce que la règle D ne doit PAS faire**
- ne pas se déclencher sur le **volume** de rejets connus : `errors_simple`
  (300 rejets métier) doit rester sans verdict d'incident (cohérence phase 5,
  `preuves/phase5-croisee-2026-11-05/SYNTHESE.md`) — seul un motif **nouveau**
  compte ;
- ne jamais conclure sur un seul échantillon (garde-fou du moteur conservé) ;
- `null` sur sondes périmées / données manquantes (critères #5/#6).

## 6. Faux amis et non-confusion

| Symptôme | Candidat D | Autre famille | Ce qui tranche |
|---|---|---|---|
| lag qui monte, aucun rejet | poison (D2) | A `consumer_backlog` | état du validator (`error` vs `running`) + rejets figés |
| rejets qui apparaissent | motif nouveau (D1) | rejets métier normaux | motif hors catalogue + signaux d'infra au vert |
| payload illisible | `malformed` | `poison` | l'endroit du décodage (poison = boucle) |

## 7. Procédure de vérification

1. `./scripts/inject-semantics-drift.sh enum` → rejet `invalid_initial_status` ;
2. `./scripts/tp-db.sh` → lire le motif (nouveau ou déjà connu ?) ;
3. `./scripts/diagnose.sh --json` → verdict actuel : **aucune famille D**
   (c'est le constat C3, fausse famille A si poison) ;
4. après intégration de la règle D : refaire 1-3 → verdict attendu
   `semantic_drift/contract_drift` (ou `contract_poison` en mode poison).

## 8. Références

- Code : `pix-validator/validation.py:11-45` (contrat),
  `pix-decision-engine/decision.py:79-104`,
  `pix-decision-engine/event_builder.py:38-51`,
  `persister-rejected/main.py:146,189-190`,
  `service-health/probes.py:220-228`, `diagnose_engine.py:97,110-112`.
- Simulateur : [`scripts/inject-semantics-drift.sh`](../../scripts/inject-semantics-drift.sh).
- Docs liées : [`pix-validation-stop-protocol.md`](pix-validation-stop-protocol.md),
  [`decision-sla.md`](decision-sla.md),
  [`diagnostic-engine-design.md`](diagnostic-engine-design.md),
  [`../contrats-evenements/`](../contrats-evenements/),
  [`../../TP/semantiques-kafka-en-une-page.md`](../../TP/semantiques-kafka-en-une-page.md).
