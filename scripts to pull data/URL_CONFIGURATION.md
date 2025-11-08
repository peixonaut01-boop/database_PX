# IBGE URL Configuration

## Important Note

Each IBGE table may have different URL parameters. The URL pattern used in the scripts follows this structure:

```
https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela{TABLE_NUMBER}.xlsx&terr=N&rank=-&query=t/{TABLE_NUMBER}/n1/all/v/all/p/all/c11255/all/d/v583%202/l/,v%2Bc11255,t%2Bp
```

However, **different tables may require different query parameters**. 

## How to Get the Correct URL

1. Go to the IBGE SIDRA website: https://sidra.ibge.gov.br/
2. Navigate to the specific table you need
3. Configure the table parameters (variables, periods, etc.)
4. Click "Gerar tabela" (Generate table)
5. Select the XLSX format
6. Copy the generated URL from the browser

## URL Parameters Explanation

- `t/{TABLE_NUMBER}` - Table number
- `n1/all` - National level (Brazil)
- `v/all` - All variables
- `p/all` - All periods
- `c11255/all` - Classification codes (may vary by table)
- `d/v583%202` - Data dimension (may vary by table)

## Testing URLs

You can test if a URL is correct by:
1. Opening it in a browser - it should download an XLSX file
2. Running the script - check for 400 Bad Request errors
3. Using the `test_url.py` script (see below)

## Current Status

- ✅ Table 1620 - Working (verified)
- ⚠️ Other tables - URLs need to be verified and may need adjustment

## Next Steps

1. Verify each table URL manually from IBGE SIDRA
2. Update the URLs in each script file
3. Test each script individually
4. Update this document with verified URLs

