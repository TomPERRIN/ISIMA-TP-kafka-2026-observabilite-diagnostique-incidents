# Lecture de la vue Grafana du diagnostic — notes

> **Objet** : lire la frise et les panneaux du dashboard [`simul-pix-diagnostic`](../../infra/observability/grafana/dashboards/)
> pendant une démo — les couleurs, ce que chaque panneau affiche, et les deux pièges corrigés le 09/11/2026.
>
> La CLI `diagnose.sh` reste la **source de vérité** : la vue *affiche* le diagnostic, elle ne le *prouve* pas.

---

## 1. La frise : une couleur par famille

| Couleur | Famille | Verdict | Cause probable type |
|---|---|---|---|
| 🟢 vert | sain | `healthy` | — (aucun incident actif) |
| 🟣 violet | indéterminé | `indeterminate` | **hypothèses** affichées + vérification suivante (ex. « [hypothesis] Amont arrêté… ») |
| 🟠 orange | A | `consumer_backlog` | la file grossit (retard de lecture applicatif) |
| 🟡 jaune | B | `kafka_replication` | réplication dégradée (broker en pause / copie en retard) |
| 🔴 rouge | C | `persistence` | la base ne suit plus (persister lent / PostgreSQL en panne) |
| 🔵 bleu | D | `slow_processing` | paiement traité trop lentement (délai SLA trop court pour la charge) |
| ⬛ gris | — | — | exporteur arrêté (pas de données) |

Le contenu est **stable au sein d'une phase** (diagnostic déterministe, recalculé à chaque échantillon) et change entre les phases. Des verdicts transitoires d'autres familles peuvent apparaître (ex. un rouge bref pendant l'orange, quand PostgreSQL décroche sous le flux) — ce n'est pas une erreur : le moteur diagnostique ce qui décroche *à cet instant*.

---

## 2. Ce que chaque panneau affiche

| Panneau | Contenu | Requête (flux) |
|---|---|---|
| Frise (state_code) | une couleur par échantillon, dans le temps | `aggregateWindow(fn: last)` — temps-sensible |
| Incident probable | le libellé de la cause la plus probable | `group() → keep → sort(columns: ["_time"]) → last()` |
| Probabilité / Criticité | idem, pour `probability` / `criticality` | idem avec tri |
| Motif indéterminé | la raison de l'abstention (violet) | idem avec tri |
| Causes examinées | les causes probables **et écartées**, avec pourquoi | idem avec tri — **vide au vert** |
| Âges (payload / réplication) | fraîcheur des sondes | `aggregateWindow(fn: last)` — temps-sensibles |

Pendant un incident, « Causes examinées » se **remplit** ; au retour au vert, il se **re-vide**
(étiquette « État sain — aucun incident actif » à l'état sain).

---

## 3. Les deux pièges corrigés (09/11/2026)

### 3.1 `last()` sans tri retournait un verdict périmé

**Symptôme** : au retour au vert, l'incident restait affiché dans les panneaux alors que la
frise montrait le bon état.

**Cause** : en Flux, `group()` + `last()` **sans `sort(columns: ["_time"])`** retourne la
dernière ligne de la **dernière table fusionnée** (ordre de fusion par tag `verdict_family`),
pas le verdict le plus récent. Le moteur exporte plusieurs familles ; selon l'ordre de fusion,
`last()` tombait sur un échantillon périmé d'une autre famille.

**Fix** : `sort(columns: ["_time"])` avant `last()` dans les 5 panneaux à verdict unique
(Incident probable, Probabilité, Criticité, Motif indéterminé, Causes examinées). Les panneaux
temporels (`aggregateWindow(fn: last)`) sont temps-sensibles et n'avaient pas le bug.

**Preuve côte à côte** : sans tri → violet `11:44:59` ; avec tri → vert `11:47:01 = "—"`.

### 3.2 `last()` sautait les valeurs vides

**Symptôme** : après le masquage des causes à l'état sain (chaîne vide exportée), le panneau
restait bloqué sur la dernière cause non vide.

**Cause** : `last()` ignore les valeurs vides — si le dernier échantillon exporte `""`,
`last()` remonte à l'échantillon précédent non vide.

**Fix** : exporter une **valeur non vide** à l'état sain (`cause_label = "État sain — aucun
incident actif"`, `causes_list = "—"`) et masquer à l'affichage (mapping « — » et « aucune
cause candidate » → vide dans les overrides Grafana). Le tri (§3.1) garantit que la valeur
reçue est bien celle du verdict le plus récent.

---

## 4. Réflexes de vérification

- **Mesurer après l'arrivée des points verts** : un check trop tôt = mauvais diagnostic
  (les sondes sont fraîches mais l'incident vient de se refermer) ;
- **Rafraîchir le dashboard** après une édition du JSON : le file provisioner recharge
  automatiquement (incrémenter la version) ;
- **Douter de la vue → croiser avec la CLI** : `./scripts/diagnose.sh --watch 15` reste la
  source de vérité, et `tp-kafka.sh` / `tp-db.sh` le dernier rempart.
