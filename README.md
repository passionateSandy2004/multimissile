# MultiThreadMissile - Universal Product Extractor

Universal extractor that parses product listings from arbitrary e-commerce search/result pages using layered, comprehensive selector strategies and robust fallbacks.

## Railway Deployment

This folder is configured for standalone deployment on Railway.

### Prerequisites

- Railway account
- Supabase credentials (URL and API key)

### Environment Variables

Set these in Railway dashboard:

**Required:**
- `SUPABASE_URL` - Your Supabase project URL
- `SUPABASE_KEY` - Your Supabase anon key

**Optional Configuration:**
- `MAX_PARALLEL_WORKERS` - Number of parallel workers (default: auto-detected)
- `DB_URL_BATCH_SIZE` - Batch size for database operations (default: 1000)
- `MAX_RETRIES` - Maximum retry attempts (default: 3)
- `DRY_RUN_SAMPLE` - Sample size for dry run (default: 0)
- `DRY_RUN_ONLY` - Enable dry run only mode (default: false)
- `PARALLEL_PROGRESS_LOG` - Enable progress logging (default: true)
- `DB_URL_STATUS_FILTER` - Status filters for URLs (default: pending,retrying)
- `DB_URL_LIMIT` - Limit number of URLs to process (default: 0 = unlimited)
- `DB_URL_OFFSET` - Offset for URL processing (default: 0)
- `BULK_URLS` - Manual URLs to process (JSON array or comma-separated)
- `BULK_URLS_FILE` - Path to file containing URLs

### Deployment Steps

1. **Connect to Railway:**
   - Go to Railway dashboard
   - Create new project
   - Connect GitHub repository or upload this folder

2. **Configure Environment Variables:**
   - Add `SUPABASE_URL` and `SUPABASE_KEY` in Railway dashboard
   - Add any optional configuration variables

3. **Deploy:**
   - Railway will automatically detect the Dockerfile
   - Build will install Chrome and all dependencies
   - Application will start automatically

### How It Works

1. **Database-Driven Mode (Default):**
   - Connects to Supabase
   - Fetches URLs from `product_page_urls` table
   - Processes URLs in parallel batches
   - Saves extracted products to `r_product_data` table

2. **Manual URL Mode:**
   - Set `BULK_URLS` environment variable with URLs
   - Or set `BULK_URLS_FILE` with path to file
   - Processes provided URLs

### Resource Requirements

- **Memory:** ~2-4GB recommended (each Chrome instance uses ~200-500MB)
- **CPU:** 2-4 cores recommended for parallel processing
- **Storage:** Minimal (logs and temporary files)

### Monitoring

- Check Railway logs for progress
- Monitor Supabase database for extracted products
- Check `r_product_data` table for results

### Troubleshooting

**Build fails:**
- Check Railway logs for specific errors
- Ensure all dependencies are in requirements.txt
- Verify Dockerfile syntax

**Chrome not found:**
- Verify Chrome installation in build logs
- Check if all system dependencies are installed

**Database connection issues:**
- Verify SUPABASE_URL and SUPABASE_KEY are set correctly
- Check Supabase project status
- Verify network connectivity

**No products extracted:**
- Check if URLs are available in `product_page_urls` table
- Verify URL processing status in database
- Check application logs for errors

