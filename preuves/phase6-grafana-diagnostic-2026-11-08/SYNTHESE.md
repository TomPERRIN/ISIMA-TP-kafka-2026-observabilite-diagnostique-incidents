# Vue Grafana du diagnostic — synthèse (08/11/2026)

Vue permanente du diagnostic : le dashboard Grafana **`Simul-Pix - Diagnostic (SAÉ 5)`**
(uid `simulpix-diagnostic`) affiche la métrique `simulpix_diagnostic`, écrite par
l'exporteur [`scripts/diagnostic-export.sh`](../../../scripts/diagnostic-export.sh)
(le moteur [`diagnose.sh`](../../../scripts/diagnose.sh) est relevé vers InfluxDB).
**La CLI reste la source de vérité** : le dashboard affiche, il ne prouve pas.

## Captures — cycle incident C-postgres-indispo (scénario `nominal`)

| Capture | Ce qu'elle montre |
|---------|-------------------|
| `01-vue-saine.png` | Avant incident : frise verte (`healthy`), aucune cause nommée |
| `02-persistence.png` | Pendant l'incident : frise rouge (`persistence`), cause « Le service postgres ne répond plus (sonde en échec) », probabilité Faible / criticité Critique, causes examinées (probable + écartée) |
| `03-diagnose-cli.png` | Le même incident vu par la CLI `./scripts/diagnose.sh` (source de vérité) : cause `service_down:postgres`, preuves double lecture, prochains contrôles |

## Reproduction

```bash
./scripts/diagnostic-export.sh --watch 15   # terminal 1 — exporteur
./scripts/run-scenario.sh nominal 100 10    # terminal 2 — flux nominal
docker compose stop postgres                # incident C — indisponibilité PostgreSQL
# captures : dashboard http://localhost:3000/d/simulpix-diagnostic + CLI
./scripts/diagnose.sh                       # terminal 3 — CLI
docker compose start postgres               # retour sain
```
