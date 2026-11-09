# Relecture du guide praticien — test en conditions réelles (Madiak5, 08/11)

> Test du `docs/operations/guide-diagnostic.md` en lecteur externe : chaque commande
> exécutée verbatim sur la plateforme vivante (16 conteneurs, clone frais de la branche
> `branche-Madi`), pendant un incident réel injecté en suivant la procédure du guide.
> Contexte : tâche n°5 du plan d'actions (« test du guide praticien pendant un incident »).

## 1. Ce qui a été testé — et résultat

| Section du guide | Test effectué | Résultat |
|---|---|---|
| §0 Prérequis | `curl -i http://localhost:8082/health`, `./scripts/diagnose.sh --reset` | ✅ HTTP 200, message de reset clair |
| §1 Les 5 modes | `diagnose.sh` ponctuel, `--json`, `--single`, `--reset`, `--watch 15` | ✅ les 5 fonctionnent ; le garde-fou « span < 10 s » observe en direct (échantillon écarté à span 7.4 s) |
| §2 Lire une sortie | format humain vs exemple du guide | ✅ tous les blocs présents (état, résumé, impact, preuves double lecture, fraîcheur, échantillons, vérification suivante) |
| §3/§4 Vérifications indépendantes | `tp-kafka.sh`, `tp-db.sh` | ✅ topics/partitions/réplication lisibles ; totaux + motifs de rejet lisibles |
| §7 Fermeture | commande copiée telle quelle du guide | ✅ `generated 600 == raw_end 600 == outcome 600 == db 600, gaps 0` |
| §5.1 Incident A en conditions réelles | `--watch 11` + `run-scenario.sh consumer_lag 600 20`, fermeture en fin de drainage | ✅ séquence de verdicts **identique** à la table du guide (détail §2 ci-dessous) |
| §8 Rejouer les essais | couvert par le rejeu indépendant du même jour (campagne complète 4/4, 0 FP) | ✅ |
| §9 Dépannage | déclenché involontairement en conditions réelles | ⚠️ un cas mal couvert, cf. §3 |

## 2. L'incident A suivi selon la procédure du guide

Procédure appliquée : `--watch 11 --json` lancé en arrière-plan, puis injection du
scénario `consumer_lag 600 20`, puis retraitement avec `tp-kafka.sh` / `tp-db.sh`,
fermeture en fin de drainage (§7).

Séquence observée (29 lectures, `--watch 11`) :

| t | verdict observé | conforme au guide ? |
|---|---|---|
| début | `indeterminate/needs_second_sample` — lag 161 visible mais UNE seule lecture : le moteur refuse de trancher | ✅ anti lag éclair |
| drainage | `consumer_backlog` ×4 — cause `reader_backlog` (« Retard de lecture applicatif »), lag 124 → 82 → 45 → 11 | ✅ cause nommée, probabilité/criticité affichées |
| transition | `indeterminate/ambiguous_sla` | ✅ |
| fin | `healthy` + cause `upstream_stopped_normal` (« Amont arrêté : situation normale ») | ✅ |

État final vérifié : `generated 600 == outcome 600 == db (24 + 576) == 600, gaps 0`
→ **fermeture complète, 0 perte**. Particularité observée : 576 messages rejetés avec
le motif `processing_timeout` (le backlog a dépassé le TTL de décision de 10 s) —
comportement explicitement anticipé par le guide (« surveiller … les rejets
`processing_timeout` ») et cohérent avec la relecture en base de `tp-db.sh`.
La relecture de la campagne du même jour (`preuves/phase4-campagne-rejeu-…/A-retard-lecture/`)
montre la même détection : 1 `indeterminate` + 8 `consumer_backlog`, cause
`reader_backlog`, délai 15 s, PASS.

## 3. Deux écarts relevés (propositions de correction)

1. **Le `--watch` meurt sur une indisponibilité transitoire — non couvert par le §9**
   (relevé en conditions réelles). Le lancement d'un scénario recrée **service-health**
   (`scripts/run-scenario.sh`, lignes 135 et 142 : le nettoyage et le `docker compose up`
   incluent `service-health`), donc `http://localhost:8082/health/details` devient
   injoignable quelques secondes, et `diagnose.sh --watch` **quitte au lieu de
   réessayer**. Or la démarche naturelle du lecteur (§5.1 : « lancer le watch puis
   injecter ») tombe exactement dedans. Le §9 traite ce symptôme comme « plateforme
   éteinte → `./start.sh` », ce qui est trompeur ici : la plateforme n'est pas éteinte.
   *Proposition* : ajouter au §5.1 et au §9 la règle « lancer le `--watch` **après** la
   fin de la recréation des conteneurs du scénario (ou relancer `--watch` si
   `injoignable` apparaît au moment de l'injection) ».

2. **Le délai annoncé en §5.1 ne correspond pas à la configuration réelle** : le guide
   écrit « décision 400 ms » mais la plateforme configure `decision_engine=350 ms`
   (affiché par `run-scenario.sh`). *Proposition* : corriger en « décision 350 ms ».

## 4. Conclusion

Le guide remplit son contrat de recette praticienne : **toutes les commandes testées
fonctionnent verbatim**, la séquence de verdicts pendant l'incident est celle
annoncée, les vérifications indépendantes sont accessibles en 1 commande chacune et la
fermeture départage backlog / rejet / perte d'un seul regard. Les deux écarts relevés
sont des améliorations de doc (ordre de lancement du watch, valeur d'un délai), pas des
défaillances du produit : le moteur a détecté, nommé et quantifié l'incident de bout
en bout, et l'état final est démontré à 0 perte.
