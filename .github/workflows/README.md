# GitHub Actions Workflows

## Update IBGE CNT Data

This workflow automatically updates IBGE (Brazilian Institute of Geography and Statistics) data in Firebase on a scheduled basis.

### Schedule

- **Frequency**: Quarterly (every 3 months)
- **Months**: March, June, September, December (months 3, 6, 9, 12)
- **Days**: 1st through 5th of each scheduled month
- **Time**: 9:00 AM Brazilian Time (BRT, UTC-3)
- **Runs**: Twice per scheduled day
  - First run: 9:00:30 AM BRT
  - Second run: 9:01 AM BRT

### Cron Expression

```
0 12 1-5 3,6,9,12 *
```

- **0 12**: 12:00 UTC (9:00 AM BRT)
- **1-5**: Days 1 through 5
- **3,6,9,12**: Months March, June, September, December
- **\***: Any day of the week

### What It Does

1. Checks out the repository
2. Sets up Python 3.11
3. Installs dependencies from `requirements.txt`
4. Runs the IBGE data update script twice:
   - First run at 9:00:30 AM BRT (30 seconds after trigger)
   - Second run at 9:01 AM BRT (1 minute after trigger)
5. Verifies the data structure after updates

### Manual Trigger

The workflow can also be triggered manually from the GitHub Actions tab:
- Go to **Actions** → **Update IBGE CNT Data** → **Run workflow**

### Requirements

- Python 3.11
- All dependencies from `requirements.txt`
- Firebase access (data is uploaded automatically)

### Notes

- Brazilian Time (BRT) is UTC-3
- The workflow runs on Ubuntu Linux (GitHub Actions runner)
- Each run updates all 9 IBGE tables
- Data is uploaded to Firebase Realtime Database under `ibge_data/`

