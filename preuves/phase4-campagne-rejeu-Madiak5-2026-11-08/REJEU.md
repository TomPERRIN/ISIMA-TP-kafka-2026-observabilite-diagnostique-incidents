# Rejeu indépendant de la campagne phase 4

**Auteur** : Madiak5 (indépendant de l'auteur du protocole, TomPERRIN)
**Date** : 08/11/2026, machine `srv-madiakite2` (Debian, NFS, Docker rootful)
**Protocole rejoué** : `travail/notes/protocole-campagne-phase4.md` — 5 témoins nominaux + 4 incidents injectés, campagne `scripts/diagnostic-campaign.sh`.

## Conformité aux résultats d'origine (TomPERRIN, 05/11/2026)

| Critère | Rejeu (08/11) | Origine (05/11) |
|---|---|---|
| Justesse (#1) | 4/4 | 4/4 |
| Faux positifs (#4) | 0 (5 témoins, 30 échantillons) | 0 (5 témoins, 70 échantillons) |
| Délai max (#3) | 20,0 s (≤ 30 s) | 25,0 s (≤ 30 s) |
| Causes nommées | reader_backlog, replication_catchup, persister_slow, service_down:postgres | identiques |

**Conclusion** : conforme. Le protocole est exécutable par un groupe tiers et produit les mêmes conclusions.

## Écarts d'exploitation notés

- Délai de détection meilleur sur ce rejeu (20 s vs 25 s max) ; nombre d'échantillons témoin plus faible (30 vs 70) sans effet sur les verdicts.
- Deux incidents d'exploitation pendant le rejeu (coupures SSH ; arrêt de `kafka-3` resté en place après une interruption de la campagne), remise en état vérifiée (`diagnose.sh` → flux sain) avant relance complète.
- Adaptation locale NFS documentée en `2a45e87` (chmod 644 sur l'instantané de réplication, requis par root_squash).
