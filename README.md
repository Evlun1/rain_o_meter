# Rain o Meter

A small app to know how much has it recently rained and compare it to average.

The ultimate goal is to expose a hex map of France with recent rain indicators.

The MVP goal is to expose indicators of how much has it rained yesterday in Paris, in the current month and in the past 31 days compared to 1995-2024 average data. 

## Program

1. Add exploration notebooks
2. Add API backend on FastAPI
    a. Add pre-commits, uv, ruff
    b. Add source code
    c. Add unit test code
    d. Add docker compose to do some local integration testing
    e. Add CI
3. Add infra code on Terraform to deploy backend on a lambda with a private URL
4. Add small front in React TS 
    a. Add front source code
    b. Deploy front using Terraform and Vercel
