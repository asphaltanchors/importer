# Trade Show Attribution - Company-Level Tracking

## Overview

The trade show attribution system tracks leads at the **company level**, not the individual person level. This means that if someone from a company attends your trade show, any purchase made by **anyone at that company** after the show date will be attributed to that trade show lead.

## How It Works

### 1. Lead Collection
- Leads are collected at trade shows with their email addresses
- Email domains are extracted (e.g., `john@acme.com` → `acme.com`)

### 2. Company Matching
- Lead email domains are matched to companies in your customer database
- Matching uses the `domain_mapping` table which normalizes company domains
- Personal emails (@gmail.com, @yahoo.com, etc.) are excluded from company matching

### 3. Attribution Logic

**Company-Level Attribution:**
- Once a lead's email domain matches a company, **ALL orders** from **ANY customer** at that company are included in attribution
- Example: Jane from `@westlandreg.com` comes to show → Bob from `@westlandreg.com` makes a purchase → Attributed!

**Individual-Level Tracking:**
- `lead_email_is_customer`: Boolean flag indicating if the specific lead's email exists in QuickBooks
- `distinct_purchasers_count`: Number of different people at the company who made purchases after the show

### 4. Attribution Windows

Multiple time windows track conversion rates:
- **30 days**: Quick wins
- **90 days**: Short-term impact
- **180 days**: Medium-term impact  
- **365 days**: Annual impact
- **All Time**: Unlimited - tracks purchases even years after the show

## Key Fields

### In `fct_trade_show_leads`:

**Company Matching:**
- `email_domain`: Extracted from lead email
- `company_domain_key`: Normalized company domain
- `consolidated_company_name`: Matched company name
- `company_match_status`: 'matched_existing_customer', 'unmatched', 'individual_email', etc.

**Person vs Company:**
- `lead_email_is_customer`: Does THIS person's email appear in QuickBooks?
- `distinct_purchasers_count`: How many different people at the company made purchases?

**Revenue (Company-Level):**
- `revenue_30d`, `revenue_90d`, `revenue_365d`, `revenue_all_time`: Total from ALL company customers
- `orders_30d`, `orders_90d`, `orders_365d`, `orders_all_time`: Order counts

**Attribution Flags:**
- `attributed_30d`, `attributed_90d`, `attributed_365d`, `attributed_all_time`: Did company make first purchase in this window?

### In `fct_trade_show_performance`:

**Aggregated Metrics:**
- `conversions_*`: Count of companies that made their first purchase in the window
- `total_revenue_*`: Sum of ALL revenue in the window
- `conversion_rate_*_pct`: Percentage of matched leads that converted
- `revenue_per_lead_*`: Average revenue per lead collected

**Company-Level Insights:**
- `leads_who_are_direct_customers`: Leads whose exact email is in QuickBooks
- `leads_attributed_via_company_colleagues`: Attributed through co-workers at the company
- `total_distinct_purchasers`: Total unique purchasers across all matched companies

## Example Scenario

**Trade Show:** FENCE_EXPO_2025 (Oct 1, 2025)

**Lead Collected:**
- Name: Jane Smith
- Email: jane@westlandreg.com
- Company: Westland Regional

**What Happens:**

1. **Domain Extraction:** `westlandreg.com`
2. **Company Match:** Matches to "Westland Regional" in your database
3. **Attribution Tracking:** 
   - Bob Johnson (bob@westlandreg.com) places order on Oct 15, 2025
   - Sarah Williams (sarah@westlandreg.com) places order on Nov 3, 2025
   - Mike Davis (mike@westlandreg.com) places order on Dec 20, 2025

**Results:**
- `lead_email_is_customer`: FALSE (Jane's email not in QB)
- `distinct_purchasers_count`: 3 (Bob, Sarah, Mike)
- `revenue_90d`: $15,000 (Bob + Sarah's orders)
- `revenue_365d`: $45,000 (Bob + Sarah + Mike's orders)
- `attributed_90d`: TRUE (company made first purchase within 90 days)

## Why Company-Level?

In B2B sales, especially for trade shows:
1. The person who attends may not be the purchaser
2. Multiple people from same company may place orders
3. Attribution should credit the trade show for introducing the company, regardless of which employee actually purchases
4. Domain-based matching captures the full company relationship

## Deduplication

If multiple people from the same company attend the show, leads are deduplicated by email address to prevent over-counting. The system keeps one lead per unique email address.
