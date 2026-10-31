# Guide de reprise sur incident

Si un batch reste `pending`, identifier le batch et l'erreur dans les logs Airflow.

```sql
SELECT batch_id, status, processed_at
FROM public.ingest_batches ORDER BY created_at;
```

Relancer **le même** batch et la même commande dbt :

```bash
dbt build --project-dir dbt --profiles-dir dbt --vars '{"batch_id":"demo_002"}'
python scripts/mark_processed.py --batch-id demo_002
```

Ne jamais modifier manuellement `status='processed'` pour contourner une erreur.

## Scénarios à démontrer en entretien

- Rejouer un fichier identique : aucune duplication.
- Rejouer le même `batch_id` avec un contenu différent : refus.
- Recevoir un événement historique tardif : conserver la version métier la plus récente.
- Recevoir une suppression logique : exclure la commande du revenu sans perdre sa trace.
- Faire échouer dbt : batch toujours en attente, puis relance sans perte.

## Améliorations futures

Alertes, observabilité, verrouillage pour multi-exécuteurs, secrets, gros volumes, tests de performance et scénarios de reprise après interruption.
