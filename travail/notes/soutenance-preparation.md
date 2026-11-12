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

---

## 7. Plan de la soutenance et répartition

**Format** : 2-3 min chacun (~10 min au total), plan modifiable tant qu'on reste cohérent et que
deux personnes ne disent pas la même chose.

| Partie | Qui | Durée |
|---|---|---|
| Introduction | Ania | 2-3 min |
| Conception | anayel | 2-3 min |
| Développement | Madi | 2-3 min (démo live incluse) |
| Conclusion | Tom | 2-3 min |

**Les 3 frontières anti-doublon** :
1. La matrice : anayel la présente en Conception — Tom/Madi disent seulement « le moteur l'implémente », sans la ré-expliquer ;
2. Le vocabulaire : tout est posé dans l'Intro (broker, topic, consommateur, lag, persistance) — personne ne redéfinit après ;
3. Intro = *pourquoi c'est difficile* (Ania) ; Conclusion = *ce que notre solution ne couvre pas* (Tom). Ne pas mélanger.

---

## 8. Les pitches (texte parlé, ~2-3 min chacun)

### Ania — Introduction

> Bonjour. Notre SAÉ porte sur l'observabilité et le diagnostic d'incidents dans Simul-Pix,
> une plateforme de paiement simulée. Le sujet : construire une lecture d'incident qui ne
> confonde pas trois situations — un retard de consommateur, une réplication dégradée, une
> panne de persistance.
>
> La problématique tient en une phrase : **plusieurs causes produisent le même symptôme**.
> Une courbe qui monte peut venir d'un lecteur lent, d'une base saturée, ou d'une fin de
> scénario normale. Le risque : corriger la mauvaise chose — redémarrer PostgreSQL alors
> que le problème est ailleurs.
>
> La plateforme en 30 secondes : un générateur émet des paiements dans Kafka. Trois serveurs
> Kafka — des **brokers** — stockent les messages dans des files, les **topics**. Les services —
> les **consommateurs** — lisent les topics et traitent les paiements, jusqu'à l'écriture en base
> PostgreSQL. Deux notions clés pour la suite : le **lag**, c'est le retard de lecture d'un
> consommateur ; la **persistance**, c'est l'écriture du résultat en base.
>
> Pourquoi c'est difficile ? Nous l'avons formalisé : le diagnostic est une fonction qui prend
> des observations et doit produire une cause — ou **s'abstient explicitement** quand la donnée
> ne permet pas de trancher. Mieux vaut dire « je ne sais pas, voici quoi vérifier » que nommer
> une mauvaise cause.

### anayel — Conception

> Première étape : analyser l'existant. Le socle fournit de bons indicateurs — compteurs à
> chaque étape, lag par groupe de consommateurs, état de réplication du cluster, écritures en
> base, un point de santé. Des dashboards Grafana affichent tout ça. Mais **un indicateur n'est
> pas un diagnostic** : une courbe qui monte ne dit pas pourquoi. C'est l'écart que nous comblons.
>
> Deuxième étape : les incidents. Trois familles recensées — retard de consommateur, réplication
> dégradée, panne de persistance — plus une quatrième, le traitement lent. Et surtout les **faux
> amis** : un lag de 1 est un éclair normal, une base figée peut être la fin normale d'un scénario.
> Sans ces distinctions, chaque alerte serait un faux positif.
>
> Troisième étape : la **matrice**. Cinq lignes, une par symptôme. Chaque ligne oppose au moins
> deux causes plaisibles et nomme l'observation qui les départage. Exemple : la file grossit —
> lecteur ou base ? On tranche avec le groupe en retard et la tendance de l'écart de persistance.
> Autre exemple : l'URP monte — broker mort ou copie en retard ? On vérifie l'état du cluster
> avant de conclure. Chaque règle s'appuie sur une **mesure**, pas sur une intuition.

### Madi — Développement

> La solution : un moteur qui automatise cette matrice. À chaque échantillon, il lit l'état de
> la plateforme, applique les règles, et produit un verdict : la famille, la cause probable, les
> causes écartées et pourquoi. Et quand la donnée ne suffit pas, il **s'abstient explicitement** —
> sept raisons cataloguées, chacune avec la vérification suivante.
>
> L'usage est une commande. J'en lance une maintenant. *[Démo live : `./scripts/demo-incident.sh A`]* —
> Voici le dashboard : la frise est verte, le panneau « Causes examinées » est vide. L'incident
> arrive : la frise passe orange, le panneau se remplit avec la cause probable et les causes
> écartées. Le backlog se vide, retour au vert, le panneau se re-vide. Les causes n'existent que
> pendant l'incident.
>
> Cette vue Grafana est permanente. Un détail qui nous a beaucoup appris : en préparant cette
> démo, nous avons trouvé un **vrai bug d'affichage** — une requête retournait un verdict périmé
> d'une autre famille. Corrigé, vérifié en conditions réelles. L'observabilité a servi à corriger
> l'outil d'observabilité.

### Tom — Conclusion

> Les résultats, tous mesurés sur incidents injectés : bonne famille nommée sur **4 cas sur 4** ;
> délai de détection **entre 3 et 25 secondes**, sous la garantie de 30 ; **zéro faux positif sur 70
> échantillons** nominaux ; et la non-confusion prouvée mécaniquement — à la fermeture de chaque
> essai, le total généré égale le total persisté : 800 égale 800 égale 800, aucun écart.
>
> Soyons honnêtes sur les **limites**. Zéro faux positif sur 70 échantillons ne prouve pas zéro
> pour cent : l'intervalle de confiance monte à 5 %. Nos résultats portent sur 4 incidents
> scriptés sur une machine — pas sur des pannes réelles. Et nous n'avons jamais réussi à
> provoquer une perte de données : le socle rejoue toujours. C'est un résultat négatif, documenté
> tel quel.
>
> **Perspectives** : une détection adaptative au lieu de seuils fixes, et des pistes issues de la
> recherche — théorie de la détection du signal, loi de Little sur les files d'attente.
>
> Enfin, le **dépôt** : la documentation dans `docs/` — architecture, guide praticien pas à pas ;
> les preuves dans `preuves/` — 169 fichiers rejouables ; les tests ; et tout se rejoue en deux
> commandes. Merci — nous sommes prêts pour vos questions.

---

## 9. Plan des slides (11 slides, ~10 min)

**Deck généré** : `travail/soutenance-sae5.pptx` (rôles + contenus déjà remplis, à ouvrir dans
PowerPoint/LibreOffice pour ajuster la mise en forme).

| # | Titre | Qui | Contenu |
|---|---|---|---|
| 1 | **Titre** | tous | « SAÉ 5 — Observabilité et diagnostic d'incidents (Simul-Pix) » ; l'équipe ; la garantie en une ligne |
| 2 | **Le sujet & la problématique** | Ania | la garantie citée ; le schéma mental : une courbe qui monte → 3 explications possibles (lecteur lent / réplication / base) ; le risque : corriger la mauvaise chose |
| 3 | **La plateforme & le vocabulaire** | Ania | le schéma du pipeline (generator → Kafka raw → validator → decision → outcome → PostgreSQL) ; 5 définitions : broker, topic, consommateur, lag, persistance |
| 4 | **L'existant : des indicateurs, pas de diagnostic** | anayel | les indicateurs du socle (compteurs, lag, URP, `/health`, dashboards) ; l'écart comblé : **rien n'interprète** |
| 5 | **Les familles & les faux amis** | anayel | le tableau des 4 familles (A/B/C/D, couleur, symptôme, cause type) ; les faux amis (lag 1 = éclair normal ; base figée = fin de scénario normale) |
| 6 | **La matrice : 5 lignes** | anayel | la matrice (symptôme → 2 causes → l'observation discriminante) ; 1-2 exemples chiffrés (lag 325-517 vs 1 ; URP → vérifier le cluster avant de conclure) |
| 7 | **Le moteur automatise la matrice** | Madi | ce qu'il produit (famille, cause probable, causes écartées, abstention + vérification suivante) ; 7 raisons d'abstention ; les seuils justifiés en 1 ligne |
| 8 | **Démo live** | Madi | la commande `./scripts/demo-incident.sh A` ; ce qu'on regarde (frise orange → vert, panneau « Causes examinées ») ; *[lancer la démo ici]* |
| 9 | **La vue Grafana & le bug** | Madi | capture d'écran du dashboard ; le récit du bug en 1 ligne (last() sans tri → sort(_time) — corrigé, vérifié) |
| 10 | **Résultats mesurés** | Tom | le tableau des chiffres : 4/4 ; 3-25 s ≤ 30 s ; 0 FP / 70 (IC ≤ 5 %) ; 800 == 800 == 800 ; 37 tests |
| 11 | **Limites, perspectives & le dépôt** | Tom | ce qu'on ne promet pas (IC, 4 incidents scriptés, perte jamais provoquée) ; perspectives (adaptatif, LIMOS) ; le rejeu en 2 commandes ; « Merci — vos questions » |

**Conseils de fabrication** :
- 1 slide ≈ 1 minute de parole ; au-delà de 6 lignes de texte, le public ne lit plus ;
- les chiffres du slide 10 viennent de la section 3 de cette fiche (tous mesurés, pas promis) ;
- le slide 8 est le plus important : le lancer AVANT de parler, l'incident dure ~40 s ;
- garder les captures (dashboard avant/pendant/après incident) en secours, au cas où la
  plateforme ne tournerait pas le jour J — elles sont dans `preuves/phase6-grafana-diagnostic-2026-11-08/`.
