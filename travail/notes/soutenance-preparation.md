# Préparation de la soutenance — SAÉ 5

> **Fiche de préparation** : la démo, le récit, les chiffres clés, les questions probables.
>
> Sources : [`analyse-theorique-diagnostic.md`](../../docs/architecture/analyse-theorique-diagnostic.md),
> [`diagnostic-engine-design.md`](../../docs/architecture/diagnostic-engine-design.md) (§9 portée/limites),
> [`guide-diagnostic.md`](../../docs/operations/guide-diagnostic.md).

---

## 1. La démo (2 commandes, un terminal)

| Commande | Ce que ça montre |
|---|---|
| `./scripts/demo-incident.sh A` | un incident **seul** (orange, ~40 s) : exporteur auto + recréation + injection + retour au vert |
| `./scripts/demo-incident.sh B` / `C` | réplication dégradée (jaune) / panne PostgreSQL (rouge) |
| `./scripts/demo-diagnostic.sh` | la **campagne complète** (toutes les familles + témoins, ~35 min) — Ctrl+C entre les cas |

Déroulé à raconter pendant l'incident A :

1. *« Je lance l'incident A : la plateforme est saine, le panneau "Causes examinées" est vide »* (vert + panneau vide) ;
2. *« L'incident arrive : la frise passe orange, le panneau se remplit avec la cause probable et les causes écartées »* ;
3. *« Le backlog se vide : retour au vert, le panneau se re-vide — les causes n'existent que pendant l'incident »* ;
4. Vérification indépendante en direct si le prof creuse : `tp-kafka.sh` / `tp-db.sh`.

Dashboard : `http://localhost:3000/d/simulpix-diagnostic` (auto-refresh 5 s).
Réinitialiser entre deux démos : `./scripts/reset-scenario.sh` ; vider l'historique de la frise : suppression
`simulpix_diagnostic` dans le bucket `simulpix_observability` (API InfluxDB, `predicate` sur la mesure).

---

## 2. Le récit du bug (le détail qui montre du recul)

> *« En préparant cette démo, j'ai découvert un vrai bug d'affichage : `last()` de Flux sur une
> table `group()` **non triée** retourne la dernière ligne de la dernière table fusionnée — pas le
> verdict le plus récent. Au vert, l'incident restait affiché alors que la frise montrait le bon
> état. Corrigé avec `sort(columns: ["_time"])` dans les 5 panneaux, vérifié en conditions réelles. »*

La leçon à en tirer : **l'observabilité sert aussi à corriger l'outil d'observabilité** — et un
diagnostic affiché doit être vérifié contre la source de vérité (la CLI `diagnose.sh`).

---

## 3. Les chiffres clés (tous mesurés, pas promis)

| Critère | Résultat | Source |
|---|---|---|
| Justesse | **4/4 familles** correctes, 0 famille erronée | analyse théorique §6.2 |
| Délai de détection | **3 s – 25 s** (moyenne 14,25 s), ≤ 30 s (garantie) | §6.4 |
| Faux positifs | **0 sur 70 échantillons** nominaux (IC 95 % borne sup. ~5 %) | §6.5 |
| Abstention explicite | 7 raisons cataloguées, 3 échantillons sur 40 (7,5 %) | §4 |
| Non-confusion | backlog ≠ rejet ≠ perte : 4/4 cas croisés (800 == 800 == 800, écarts 0) | §6.6 |
| Impact métier | moteur 278 == sonde PG 278 == SQL 278, delta contrôlé 0 == 0 | phase 7 |
| Code | 37 tests unitaires du moteur | `scripts/lib/test_diagnose_engine.py` |

---

## 4. La garantie et son périmètre

**Garantie** : « Notre lecture d'incident ne doit pas confondre retard de consommateur,
réplication dégradée et panne de persistance, prouvée par une matrice validée sur incidents
injectés : bonne cause retenue sur 100 % des cas connus, état “indéterminé” + vérification
suivante sinon. »

**Ce qu'on ne promet pas** (honnêteté, conception §9 + analyse théorique §9) : 0 % de faux
positifs *universel* (IC ≤ ~5 %), pannes non testées (réseau, disque, rééquilibrage), perte de
données (jamais provoquée — résultat négatif documenté), impact métier monétaire, seuils
adaptatifs, et la vue Grafana **affiche** le diagnostic sans le **prouver** (la CLI reste la
source de vérité).

---

## 5. Questions probables et réponses courtes

| Question | Réponse courte |
|---|---|
| Pourquoi l'abstention plutôt qu'une conclusion ? | Une conclusion fausse oriente vers la mauvaise correction ; l'abstention coûte ~15 s et indique quoi faire ensuite (analyse §2.2, §4.3). |
| Pourquoi des seuils fixes ? | Explicables (chaque seuil justifié par une mesure), testables, et une baseline apprise pendant un incident risque d'absorber l'anomalie (§5.4). |
| Comment prouvez-vous la persistance ? | Jamais un seul offset : compteur en base == compteurs métier == offsets topic (`convergence_check.py`), contrôle 800 == 800 == 800, écarts 0 (§3.3, §6.6). |
| Que fait la vérification indéterminée ? | Elle croise `tp-kafka.sh` + `tp-db.sh` en direct et le contenu réel des tables — pas seulement un graphique. |
| Et si une sonde ment ? | Une sonde **périmée** est détectée (garde-fou 75 s) ; une sonde qui ment en restant fraîche échappe au moteur — la vérification indépendante est le dernier rempart, c'est pourquoi elle est imprimée dans chaque verdict (conception §9). |
| Un traitement lent, c'est un backlog ? | Un traitement long **engendre** une attente grossissante, mais les causes sont tranchées par l'état de la file (v2 : familles `slow_processing` et `consumer_backlog` séparées). |

---

## 6. Actions restantes

- **Veille de la soutenance** : passer le dépôt en **PUBLIC** + transmettre l'URL à l'enseignant (TODO §12).
- Fusion de la PR #8 par Tom (raccourci incident A à ~40 s).
- Échange avec l'enseignant/LIMOS : axes SIC et ODPS, pistes de lecture (analyse théorique §10).
