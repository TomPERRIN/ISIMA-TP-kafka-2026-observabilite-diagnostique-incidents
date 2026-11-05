# Guide de diagnostic Simul-Pix — quoi faire pendant un incident

> Recette pas à pas, écrite **pour un praticien qui découvre l'incident** (un autre groupe,
> un collègue, un exploitant). Elle assume le socle fourni (météo, Grafana, alertes,
> `tp-kafka.sh`, `tp-db.sh`) et l'apport du sujet : la commande `scripts/diagnose.sh`,
> qui relie les signaux, nomme la cause et indique la vérification indépendante.
>
> Sources vivantes : matrice `travail/notes/matrice-symptomes-causes.md`, protocoles
> d'essais `travail/notes/protocole-campagne-phase4.md` et
> `travail/notes/protocole-phase5-croisee.md`, preuves `preuves/phase4-*` et
> `preuves/phase5-*`. Choix techniques : `docs/architecture/diagnostic-engine-design.md`.

---

## 0. Prérequis

1. La plateforme tourne : `./start.sh` puis `curl -i http://localhost:8082/health`
   (attendre HTTP 200 ; sous Windows, passer par WSL — le montage `${PWD}` est
   illisible depuis Git Bash).
2. Bash + Python 3 (le moteur et le CLI sont du Python pur, sans dépendance).
3. Pour une observation fraîche, repartir d'un cache vide :
   `./scripts/diagnose.sh --reset`.

---

## 1. Le réflexe de base : une seule commande

```bash
./scripts/diagnose.sh                # diagnostic ponctuel (stdout lisible)
./scripts/diagnose.sh --watch 15     # boucle toutes les 15 s — le réflexe pendant un incident
./scripts/diagnose.sh --json         # même verdict en JSON (pour un relevé/l'autre outil)
./scripts/diagnose.sh --single       # sans état : les règles à 2 échantillons restent « indéterminé »
./scripts/diagnose.sh --reset        # vide le cache d'état (retour d'une plateforme calme)
```

Règle d'usage : **l'intervalle de `--watch` doit être ≥ 10 s** (11 à 15 s). Une cadence
sous 10 s ne permet jamais aux règles qui comparent deux échantillons de trancher —
ce n'est pas un bug, c'est le garde-fou anti « lag éclair » (voir §6).

---

## 2. Lire une sortie

Exemple réel (plateforme calme, fin de scénario) — format humain par défaut :

```text
==============================================================
DIAGNOSTIC SIMUL-PIX — échantillon 2026-11-05T16:00:10.464531+00:00
état      : flux sain (aucune cause nommée)
cause     : upstream_stopped_normal — Amont arrêté (générateur terminé)...
            probabilité Élevée · criticité Faible
résumé    : Plus aucune écriture en base ET l'amont stagne : fin de scénario...
note      : Base figée expliquée : generated stagne, aucun risque de perte
preuves (double lecture) :
  [postgres] counts : db_validated_count=0.0 db_rejected_count=300.0
  [platform] counts : generated=300.0 pending_result_count=0.0 validated=0.0
  [kafka   ] consumer_groups lag_total : pix-validator 0 ; ...
fraîcheur : payload 0.14 s · réplication 25.46 s (limite 75.0 s) — OK
échantillons : 2 (span 29.86 s)
vérification à faire ensuite :
  $ Surveillance continue : ./scripts/diagnose.sh --watch 15
==============================================================
```

Table de lecture :

| Bloc | Ce qu'il dit | Réflexe |
|---|---|---|
| `état` | un des 5 verdicts (§3) | si ≠ `healthy` : §4 |
| `cause` + probabilité/criticité | la cause nommée UNIQUEMENT si l'état n'est pas `indeterminate` | `criticité Critique/Élevée` = examiner en premier |
| `preuves (double lecture)` | les signaux de chaque source (kafka / postgres / platform) | vérifier sur les sources, pas sur un dashboard |
| `fraîcheur` | âge du payload et de l'instantané réplication (limite 75 s) | âge > limite = sondes périmées : **rien ne conclut** (verdict `indeterminate/probes_stale`), relire après rétablissement |
| `échantillons (span)` | combien de lectures, espacées de combien, fondent le verdict | span < 10 s → impossible de trancher un compteur qui monte |
| `vérification à faire ensuite` | la prochaine action concrète | l'exécuter |

---

## 3. Les 5 verdicts et les causes nommées par famille

| Verdict (famille) | Causes nommées possibles | Cause critique en priorité | Vérification indépendante |
|---|---|---|---|
| `healthy` | `upstream_stopped_normal` (fin de scénario, PAS un incident) | — | `./scripts/tp-db.sh` (base figée expliquée) |
| `consumer_backlog` (A) | `reader_backlog`, `upstream_backlog_sla`, `sla_config` | — | `./scripts/tp-kafka.sh` |
| `kafka_replication` (B) | `broker_down` (Faible prob./criticité Élevée), `replication_catchup` (Élevée/Faible) | `broker_down` d'abord | `./scripts/tp-kafka.sh` (+ relire ~30 s) |
| `persistence` (C) | `postgres_unavailable` (Moyenne/**Critique**), `persister_slow` (Moyenne/Élevée) | `postgres_unavailable` en premier | `./scripts/tp-db.sh` |
| `indeterminate` | raison cataloguée, cf. §6 | — | suivre `next_check` |

Règle d'examen de la matrice : **la cause la plus critique d'abord si sa vérification
est rapide** (tp-kafka/tp-db = instantanés), puis confirmer la cause la plus probable.
C'est le tri déjà dans l'ordre de `plausible_causes[]` du JSON.

---

## 4. La procédure en 4 pas pendant un incident

1. **À l'apparition du symptôme** (météo dégradée, alerte Grafana, courbe inhabituelle) :
   `./scripts/diagnose.sh --watch 15` et LAISSER tourner tout au long de l'incident ;
2. **Lire le verdict** (§2) et, si `indeterminate`, la `raison` (§6) ;
3. **Retrancher la cause nommée** avec la vérification indépendante indiquée :
   `./scripts/tp-kafka.sh` (offsets, facteur de réplication, URP, lag par groupe) et/ou
   `./scripts/tp-db.sh` (totaux des deux tables, motifs de rejet) ;
4. **Archiver le relevé** : sortie JSON horodatée dans `preuves/` (ce que les scripts
   de campagne font automatiquement, cf. §8).

**Règles d'or de la matrice** : ne jamais conclure sur une seule lecture (2 échantillons
espacés ≥ 10 s) ni sur des sondes périmées (âge > 75 s) ; jamais depuis un dashboard
seul ; jamais sur un offset Kafka seul (toujours lire PostgreSQL).

---

## 5. Les trois situations de la mission — chaque situation a ses preuves archivées

### 5.1 Retard de consommateur (situation A) — preuves `preuves/phase5-…/A-draine/`

Injecté par : `./scripts/run-scenario.sh consumer_lag 600 20` (décision 400 ms à 20/s).

Ce qui se passe (relevé réel, cadence 11 s) :

| t (s) | verdict | fait |
|---|---|---|
| 0 | `indeterminate` (`needs_second_sample`) | lag déjà 325 mais UNE seule lecture : le moteur refuse de trancher (anti lag éclair) |
| 11 | `consumer_backlog` / `reader_backlog` | « Lag de lecture persistant/montant (325 ≥ seuil 10) » |
| 22-110 | `consumer_backlog` ×9 | pic de lag 517 puis drainage progressif |
| ~140 | `indeterminate` (`ambiguous_sla`) | transition : SLA en hausse avec file déjà vide |
| 150+ | `healthy` | fin de scénario expliquée (`upstream_stopped_normal`) |

Retranchement attendu : `tp-kafka.sh` montre le **groupe lecteur** en retard
(`pix-decision-engine` ici), la chaîne en aval est à jour. À la fermeture, la base a
rejoint le flux (fermeture, §7) : **le backlog était transitoire, 0 perte**.

Que faire : identifier le groupe en retard dans `tp-kafka.sh`, surveiller la file
(`rates`, `sla_breaches`) et les rejets `processing_timeout` (dépassement du TTL de
décision 10 s) — le remède dépend du scénario, pas du diagnostic.

### 5.2 Réplication dégradée (situation B) — preuves `preuves/phase5-…/B-replication-croise/`

Injecté par : `SIMULPIX_REPLICATION_OUTAGE_SECONDS_OVERRIDE=20
SIMULPIX_REPLICATION_PAUSED_BROKER_OVERRIDE=kafka-3 ./scripts/run-scenario.sh replication_lag 600 20`.

- verdict : `kafka_replication` (cause `replication_catchup`), en **≤ 30 s** (le cycle
  de rafraîchissement du snapshot réplication de la météo fait partie du délai) ;
- retranchement : `tp-kafka.sh` en direct — **3/3 brokers, offline=0, contrôleur actif,
  URP>0** = retard de copie (bénin, se résorbe). Si au contraire un broker est **absent**
  → `broker_down` (grave avec RF=3 / min ISR=2) : la même commande tranche.

### 5.3 Incident PostgreSQL (situation C, indisponibilité) — preuves `preuves/phase5-…/C-rattrapage/`

Injecté par : `docker compose stop postgres` 40 s au milieu d'un flux (`nominal 800 15`).

- détection rapide (≤ 10 s) : `persistence` / cause **`postgres_unavailable`**
  (criticité **Critique** — première à écarter), complétée par `service_down:postgres` ;
  `tp-db.sh` échoue pendant la panne, comme prédit ;
- au retour de PG : les persisters **rejouent** (topic + retry). Vérifier le rattrapage
  avec la fermeture (§7) : db=800+0 / outcome=800 / écarts=0 → **0 perte mesurée**.
- la variante « lent » (persisters 350 ms) est couverte par `persister_slow` : le
  discriminateur « l'écart qui S'ACCROÎT » (`persistence_gap_valid` passé de 3 à 319
  entre deux lectures espacées de 11 s).

### 5.4 Rejets métier (le faux ami N°1) — preuves `preuves/phase5-…/rejets-metier/`

Injecté par : `./scripts/run-scenario.sh errors_simple 300 10` (300 messages → 300 rejets métier).

- le moteur sonne **AUCUN incident** (verdicts `healthy`, cause `upstream_stopped_normal`
  à la fin) — les rejets partent en base, ils ne sont PAS traités comme des incidents ;
- cohérence compteur ↔ base : `rejected=300` côté compteurs métier == `300` lignes dans
  `rejected_transactions` (lire `tp-db.sh` — le motif `processing_timeout` y apparaîtra
  si un backlog a dépassé le TTL).

**Moralité** : un ratio de rejets élevé N'EST PAS un incident système tant que chaque
décision est persistée (fermeture ✔, §7). Le diagnostic est construit pour NE PAS lever
« alerte rouge » sur des rejets métier : un rejet est une donnée persistée, pas une panne.

---

## 6. `indeterminate` : pourquoi et quoi faire

Le moteur refuse de conclure quand la donnée ne le permet pas — **c'est une sortie
attendue, précieuse**. Raisons cataloguées :

| Raison | Signification | Vérification suivante |
|---|---|---|
| `needs_second_sample` | une seule lecture : aucun delta possible | relire à ~15 s (`--watch 15`) |
| `probes_stale` | sondes périmées (> 75 s) : le système d'observation peut être en panne | vérifier la collecte, re-lire |
| `data_missing` | champ attendu absent du payload | vérifier la sonde / le socle |
| `contradictory_signals` | la logique se contredit entre sources | relire ; lire tp-kafka/tp-db à la main |
| `ambiguous_sla` | SLA en hausse avec file vide et latence OK | relire à ~15 s, lire `latency` + `sla_breaches` |
| `ambiguous_non_dominated` | aucun dominateur de cause plausible | attendre, relire (du mouvement tranche) |
| `probe_blinded` / `service_down_transversal` | la panne éteint le système qui la voit | re-démarrer la sonde [1] |

[1] Ex. `stop metrics-collector` ne gèle PAS `/health/details` (le timestamp est ré-émis
à chaque requête) et l'indisponibilité du service observé est relevée comme `unreachable`
du côté des sondes de service : jamais une conclusion, toujours « indéterminé ».

---

## 7. Trancher backlog ≠ rejet ≠ perte : la fermeture

Trois réalités se ressemblent côté métrique (de la donnée en attente quelque part) —
c'est la **fermeture** de la chaîne qui les départage d'un seul regard :

```bash
# après drainage d'un incident, comparer le stock et la base (fermeture complète) :
curl -fsS http://localhost:8082/health/details \
  | python3 -c 'import json,sys; p=json.load(sys.stdin); c=p["metrics"]["counts"]; g=p["metrics"]["gaps"]
print("generated", c["generated"], "| raw_end", c["raw_topic_end_offsets"],
      "| outcome", c["outcome_count"], "| db", c["db_validated_count"]+c["db_rejected_count"],
      "| gaps", g["persistence_gap_valid"]+g["persistence_gap_rejected"])'
```

Lecture (contrôle de fermeture codé dans `scripts/lib/convergence_check.py`, utilisé
par `cross-check.sh`) :

- **fermeture** (`generated == raw_end == validated+rejected == outcome == db`, gaps 0)
  → TOUT est persisté : le backlog était transitoire, **0 perte** ;
- **écart de persistance > 0 qui résorbe** (décroissant à 2 relectures) → le stock
  rattrape la base : normal en cours de flux (rattrapage, pas une perte) ;
- **écart STABLE après file vide** = signature d'une perte — relire deux fois, puis
  confirmer avec `tp-db.sh` : les totaux n'ont pas bougé. C'est le discriminateur de la
  matrice (« l'écart qui S'ACCROÎT sans jamais se résorber »). En l'état du socle, les
  persisters **rejouent toujours** : aucune perte n'a jamais été mesurée, et ce résultat
  négatif est documenté tel quel dans le protocole phase 5 (il n'est pas supposé).
- **rejet métier** : les lignes dans `rejected_transactions` (dont les TTL
  `processing_timeout` d'un gros backlog) SONT de la donnée, persistée ; cohérence
  `rejected == lignes en base` = pas une perte (§5.4).

---

## 8. Rejouer les essais (campagne + vérification croisée)

```bash
# phase 4 — incidents + témoin nominal, mesures de garantie (SYNTHESE.md à la fin)
./scripts/diagnostic-campaign.sh          # ~40 min ; preuves dans preuves/phase4-…/

# phase 5 — croisement Kafka/PG + démonstration de non-confusion (4 cas)
./scripts/cross-check.sh                  # ~25 min ; preuves dans preuves/phase5-…/

# sous-ensemble des cas croisés si besoin :
./scripts/cross-check.sh --cases A-draine,rejets-metier
```

Résultats de référence (ceux qu'un autre groupe doit retrouver) : phase 4 = justesse
**4/4**, délai **max 25 s**, faux positifs **0/70** ; phase 5 = **4/4 PASS** avec
fermeture de chaque état final. Protocoles complets dans `travail/notes/`.

---

## 9. Dépannage

| Symptôme | Cause probable | Action |
|---|---|---|
| `diagnose: /health/details injoignable` | plateforme éteinte | `./start.sh`, attendre service-health |
| `indeterminate/needs_second_sample` persistant | cadence < 10 s | `--watch 15` au lieu d'enchaîner des ponctuels |
| `indeterminate/probes_stale` | collecteur/service-health accroché | `./stop.sh && ./start.sh`, puis `--reset` |
| lag `=1` isolé en nominal | rattrapage éclair | ne pas conclure : 0-1 en nominal est normal, le moteur l'ignore |
| verdicts restent à `needs_second_sample` | cache reparti de zéro | 2 lectures espacées nécessaires ; laisser `--watch` tourner |

---

## 10. Ce que la garantie couvre — et ne couvre pas

**Garantie** : la lecture d'incident ne doit pas confondre retard de consommateur,
réplication dégradée et panne de persistance, prouvée par la matrice validée sur
incidents injectés (justesse 4/4, délai max 25 s, 0 faux positif sur 5 témoins,
état `indeterminate` + vérification suivante sinon) — **et le diagnostic ne confond pas
backlog, rejet métier attendu et perte de données** (`cross-check.sh` 4/4).

**Couvert** : les 3 situations A/B/C + la variante « persister lent » ; la cause nommée
avec probabilité/criticité et ordre d'examen ; l'état `indeterminate` justifié avec la
vérification suivante ; le garde-fou de fraîcheur (jamais de conclusion sur sondes âgées,
y compris quand l'incident éteint le système d'observation) ; la vérification
indépendante pointée dans chaque verdict ; le délai de détection garanti (B inclut le
cycle lent de rafraîchissement du snapshot réplication, mesuré pendant la campagne).

**Non couvert (honnêteté)** :

- pas de vue HTTP/Grafana du moteur — la CLI est le produit, l'extension est différée ;
- seuils **fixes** — la détection adaptative n'est pas exigée et n'est pas implémentée
  (tous les seuils sont réglables via `SIMULPIX_DIAG_*` si la plateforme évolue) ;
- **aucune perte n'a pu être provoquée** : les persisters du socle rejouent toujours —
  la branche « écart stable = perte » est codée (fermeture non atteinte à l'échéance)
  mais jamais déclenchée en campagne ; ce résultat négatif est documenté, pas supposé ;
- le moteur ne rajoute PAS de sonde : il joue sur les signaux du socle — sa valeur
  ajoutée est le **croisement** et la discipline d'examen, pas une disponibilité
  supplémentaire de mesures.
