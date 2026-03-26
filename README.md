# Retail Promotion Effectiveness Analysis

End-to-end retail promotion effectiveness case study using SQL, PostgreSQL, Python ETL, and Power BI.

## Overview

This project evaluates the effectiveness of retail promotions using the Complete Journey dataset. The analysis was designed to measure campaign performance, category responsiveness, coupon redemption behavior, and household-level promotional response through a structured analytical workflow built in PostgreSQL and translated into a Power BI dashboard.

## Business Objective

The main objective of this project was to identify which promotional initiatives generate stronger commercial outcomes and where the clearest opportunities exist to improve promotional investment. The analysis focused on campaign effectiveness, category-level response, coupon behavior, and differences across customer segments.

## Dataset

This case study was built using the Complete Journey retail dataset, integrating campaign, transaction, product, coupon, redemption, promotion, and demographic information into a relational analytical structure.

## Tech Stack

- SQL
- PostgreSQL
- Python
- Power BI

## Methodology

The project followed four main stages:

1. Data extraction, cleaning, and validation in Python  
2. Relational data modeling and loading into PostgreSQL  
3. SQL-based analytical framework to answer the business questions  
4. Power BI dashboard development connected to the PostgreSQL analytical database  

## Key Analytical Questions

- Which promotions, campaigns, or coupons are associated with stronger outcomes in sales, purchase frequency, and basket value?
- Which categories and products appear to be the most responsive to promotional activity?
- How does promotional response vary across different households or customer segments?
- What differences can be observed between promotion-exposed households and non-exposed households?
- Which initiatives appear to drive meaningful incremental performance, and which ones show limited or inconsistent results?
- Where are the clearest opportunities to improve promotional investment across categories, products, or customer groups?
- What coupon redemption patterns help explain stronger or weaker commercial response?

## Key Findings

- Type A campaigns showed the strongest scalable performance signals.
- Soft Drinks and Yogurt emerged as the clearest category-level opportunities for additional promotional support.
- Exposed households significantly outperformed non-exposed households, mainly through higher purchase frequency.
- Coupon behavior was driven more by initial redemption than by repeated post-redemption purchase activity.
- Several findings should be interpreted as directional due to sparse promotional exposure in parts of the sampled data.

## Dashboard

A two-page Power BI dashboard was developed and connected directly to the PostgreSQL analytical database.

**Page 1 – Executive Summary**
- KPI overview
- campaign performance
- category opportunity matrix
- exposed vs. non-exposed household comparison

**Page 2 – Diagnostic and Detail**
- campaign-level incremental analysis
- household-segment response
- coupon behavior and supporting diagnostics
