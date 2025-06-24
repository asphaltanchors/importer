#!/usr/bin/env python3
"""
Domain Consolidation Analysis

This script analyzes email domains from the QuickBooks data to create
company consolidation rules based on email domains.

Phase 1: Extract and analyze domains, create normalization rules
"""

import os
import re
from collections import defaultdict
from typing import Dict, List, Tuple, Optional
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

# Load environment
load_dotenv()
DATABASE_URL = os.environ["DATABASE_URL"]

def get_db_connection():
    """Get database connection"""
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

def load_individual_domains() -> set:
    """
    Load individual email domains from config file
    """
    config_file = os.path.join(os.path.dirname(__file__), 'individual_email_domains.txt')
    
    if not os.path.exists(config_file):
        # Create default file if it doesn't exist
        default_domains = [
            'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com',
            'aol.com', 'icloud.com', 'me.com', 'msn.com',
            'comcast.net', 'verizon.net', 'sbcglobal.net', 'att.net',
            'optonline.net', 'live.com', 'bellsouth.net', 'cox.net',
            'mac.com', 'earthlink.net', 'charter.net', 'roadrunner.com'
        ]
        
        with open(config_file, 'w') as f:
            f.write("# Individual Email Domains\n")
            f.write("# Add one domain per line. Lines starting with # are ignored.\n")
            f.write("# These domains will be flagged as individual accounts, not companies.\n\n")
            for domain in sorted(default_domains):
                f.write(f"{domain}\n")
        
        print(f"Created default individual domains config: {config_file}")
    
    # Read domains from file
    domains = set()
    with open(config_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                domains.add(line.lower())
    
    return domains

def extract_primary_domain(email: str) -> Optional[str]:
    """
    Extract the primary domain from an email, handling semicolon-separated lists
    """
    if not email or email.strip() == '':
        return None
    
    # Handle semicolon-separated emails (take the first one)
    first_email = email.split(';')[0].strip()
    
    # Extract domain part
    if '@' not in first_email:
        return None
    
    domain = first_email.split('@')[1].strip().lower()
    return domain if domain else None

def normalize_domain(domain: str) -> str:
    """
    Apply domain normalization rules
    """
    if not domain:
        return domain
    
    # Skip Amazon marketplace (no visibility)
    if domain == 'marketplace.amazon.com':
        return 'SKIP_AMAZON_MARKETPLACE'
    
    # Load individual email domains from config file
    individual_domains = load_individual_domains()
    if domain in individual_domains:
        return f'INDIVIDUAL_{domain.upper()}'
    
    # Fastenal consolidation rules
    if 'fastenal.com' in domain:
        return 'fastenal.com'
    
    # Government domains - keep specific for now but could consolidate later
    gov_suffixes = ['.gov', '.mil', '.edu']
    for suffix in gov_suffixes:
        if domain.endswith(suffix):
            return domain  # Keep government domains specific for now
    
    # Default: return as-is
    return domain

def normalize_customer_name(customer_name: str) -> str:
    """
    Apply customer name normalization rules to standardize company names
    """
    if not customer_name or customer_name.strip() == '':
        return customer_name
    
    # Start with the trimmed name
    normalized = customer_name.strip()
    
    # Remove common suffixes that indicate customer types (case insensitive)
    suffixes_to_remove = [
        r'\s+End\s+User\s*$',
        r'\s+Customer\s*$', 
        r'\s+Client\s*$',
        r'\s+End\s+User\s*,\s*$',
        r'\s+Customer\s*,\s*$',
        r'\s+Client\s*,\s*$'
    ]
    
    for suffix_pattern in suffixes_to_remove:
        normalized = re.sub(suffix_pattern, '', normalized, flags=re.IGNORECASE)
    
    # Additional cleaning rules
    # Remove extra whitespace
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    
    # Remove trailing commas
    normalized = re.sub(r',\s*$', '', normalized)
    
    return normalized

def load_customer_name_normalization_rules() -> Dict[str, str]:
    """
    Load customer name normalization rules from config file
    Returns a dictionary of explicit mappings for complex cases
    """
    config_file = os.path.join(os.path.dirname(__file__), 'customer_name_mappings.txt')
    
    if not os.path.exists(config_file):
        # Create default file with examples
        default_mappings = [
            "# Customer Name Mappings",
            "# Format: original_name -> normalized_name",
            "# Lines starting with # are ignored",
            "# Add specific mappings for complex normalization cases",
            "",
            "# Example:",
            "# ZKxKZ LLC  End User -> ZKxKZ LLC",
            "# Acme Corp Customer -> Acme Corp"
        ]
        
        with open(config_file, 'w') as f:
            f.write('\n'.join(default_mappings))
        
        print(f"Created default customer name mappings config: {config_file}")
    
    # Read mappings from file
    mappings = {}
    with open(config_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '->' in line:
                original, normalized = line.split('->', 1)
                mappings[original.strip()] = normalized.strip()
    
    return mappings

def analyze_domains():
    """
    Analyze email domains from the customers table
    """
    print("Phase 1: Domain Extraction & Normalization")
    print("=" * 50)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get customer data with emails
    query = """
    SELECT 
        quick_books_internal_id,
        customer_name,
        company_name,
        main_email,
        cc_email,
        CAST(NULLIF(TRIM(current_balance), '') AS NUMERIC) as current_balance
    FROM raw.customers 
    WHERE (main_email IS NOT NULL AND main_email != '')
       OR (cc_email IS NOT NULL AND cc_email != '')
    """
    
    cursor.execute(query)
    customers = cursor.fetchall()
    
    print(f"Found {len(customers)} customers with email addresses")
    
    # Analyze domain patterns
    domain_stats = defaultdict(lambda: {
        'count': 0,
        'customers': [],
        'total_balance': 0,
        'company_names': set()
    })
    
    normalization_mapping = {}
    
    for customer in customers:
        # Process main_email
        if customer['main_email']:
            domain = extract_primary_domain(customer['main_email'])
            if domain:
                normalized = normalize_domain(domain)
                normalization_mapping[domain] = normalized
                
                domain_stats[normalized]['count'] += 1
                domain_stats[normalized]['customers'].append(customer['quick_books_internal_id'])
                if customer['current_balance']:
                    domain_stats[normalized]['total_balance'] += float(customer['current_balance'])
                if customer['company_name']:
                    domain_stats[normalized]['company_names'].add(customer['company_name'])
        
        # Process cc_email  
        if customer['cc_email']:
            domain = extract_primary_domain(customer['cc_email'])
            if domain:
                normalized = normalize_domain(domain)
                normalization_mapping[domain] = normalized
                
                # Don't double-count if same customer already counted via main_email
                if customer['quick_books_internal_id'] not in domain_stats[normalized]['customers']:
                    domain_stats[normalized]['count'] += 1
                    domain_stats[normalized]['customers'].append(customer['quick_books_internal_id'])
                    if customer['current_balance']:
                        domain_stats[normalized]['total_balance'] += float(customer['current_balance'])
                if customer['company_name']:
                    domain_stats[normalized]['company_names'].add(customer['company_name'])
    
    # Print analysis results
    print("\nTop Consolidated Domains by Customer Count:")
    print("-" * 70)
    sorted_by_count = sorted(domain_stats.items(), key=lambda x: x[1]['count'], reverse=True)
    
    for domain, stats in sorted_by_count[:20]:
        company_sample = ' | '.join(list(stats['company_names'])[:3])
        if len(stats['company_names']) > 3:
            company_sample += f" (+{len(stats['company_names'])-3} more)"
        
        print(f"{domain:30} | {stats['count']:3d} customers | ${stats['total_balance']:>8.0f} | {company_sample}")
    
    print("\nTop Consolidated Domains by Revenue:")
    print("-" * 70)
    sorted_by_revenue = sorted(domain_stats.items(), key=lambda x: x[1]['total_balance'], reverse=True)
    
    for domain, stats in sorted_by_revenue[:15]:
        if stats['total_balance'] > 0:  # Only show domains with revenue
            company_sample = ' | '.join(list(stats['company_names'])[:3])
            if len(stats['company_names']) > 3:
                company_sample += f" (+{len(stats['company_names'])-3} more)"
            
            print(f"{domain:30} | {stats['count']:3d} customers | ${stats['total_balance']:>8.0f} | {company_sample}")
    
    # Show normalization examples
    print("\nSample Domain Normalization Mappings:")
    print("-" * 50)
    interesting_mappings = {k: v for k, v in normalization_mapping.items() if k != v}
    for original, normalized in list(interesting_mappings.items())[:10]:
        print(f"{original:40} → {normalized}")
    
    print(f"\nSummary:")
    print(f"- Total unique original domains: {len(normalization_mapping)}")
    print(f"- Total consolidated domains: {len(domain_stats)}")
    print(f"- Domains with normalization applied: {len(interesting_mappings)}")
    
    # Identify high-impact consolidations
    print(f"\nHigh-Impact Consolidations (>5 customers):")
    print("-" * 50)
    for domain, stats in sorted_by_count:
        if stats['count'] > 5 and not domain.startswith('INDIVIDUAL_'):
            print(f"{domain:30} | {stats['count']:3d} customers | ${stats['total_balance']:>8.0f}")
    
    conn.close()
    
    return domain_stats, normalization_mapping

def create_domain_mapping_table():
    """
    Create a domain mapping table in the database for DBT to use
    """
    print("\nCreating domain mapping table for DBT...")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get all unique domains from customer emails
    query = """
    SELECT DISTINCT 
        CASE 
            WHEN main_email LIKE '%;%' THEN SPLIT_PART(TRIM(SPLIT_PART(main_email, ';', 1)), '@', 2)
            ELSE SPLIT_PART(TRIM(main_email), '@', 2)
        END as original_domain
    FROM raw.customers 
    WHERE main_email IS NOT NULL 
      AND main_email != ''
      AND main_email LIKE '%@%'
    
    UNION
    
    SELECT DISTINCT 
        CASE 
            WHEN cc_email LIKE '%;%' THEN SPLIT_PART(TRIM(SPLIT_PART(cc_email, ';', 1)), '@', 2)
            ELSE SPLIT_PART(TRIM(cc_email), '@', 2)
        END as original_domain
    FROM raw.customers 
    WHERE cc_email IS NOT NULL 
      AND cc_email != ''
      AND cc_email LIKE '%@%'
    """
    
    cursor.execute(query)
    domains = cursor.fetchall()
    
    # Create mapping table
    cursor.execute("DROP TABLE IF EXISTS raw.domain_mapping")
    cursor.execute("""
        CREATE TABLE raw.domain_mapping (
            original_domain VARCHAR(255) PRIMARY KEY,
            normalized_domain VARCHAR(255) NOT NULL,
            domain_type VARCHAR(50) NOT NULL,
            created_date TIMESTAMP DEFAULT NOW()
        )
    """)
    
    # Insert mappings
    for row in domains:
        original = row['original_domain'].lower() if row['original_domain'] else None
        if original:
            normalized = normalize_domain(original)
            
            # Determine domain type
            if normalized.startswith('INDIVIDUAL_'):
                domain_type = 'individual'
            elif normalized == 'SKIP_AMAZON_MARKETPLACE':
                domain_type = 'skip'
            else:
                domain_type = 'corporate'
            
            cursor.execute("""
                INSERT INTO raw.domain_mapping (original_domain, normalized_domain, domain_type)
                VALUES (%s, %s, %s)
                ON CONFLICT (original_domain) DO UPDATE SET
                    normalized_domain = EXCLUDED.normalized_domain,
                    domain_type = EXCLUDED.domain_type,
                    created_date = NOW()
            """, (original, normalized, domain_type))
    
    conn.commit()
    
    # Show summary
    cursor.execute("SELECT domain_type, COUNT(*) FROM raw.domain_mapping GROUP BY domain_type")
    summary = cursor.fetchall()
    
    print("Domain mapping table created:")
    for row in summary:
        print(f"  {row['domain_type']}: {row['count']} domains")
    
    conn.close()

def analyze_customer_names():
    """
    Analyze customer names and show normalization results
    """
    print("\nCustomer Name Analysis")
    print("=" * 50)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get all customer names
    query = """
    SELECT DISTINCT 
        customer_name,
        company_name,
        COUNT(*) as frequency
    FROM raw.customers 
    WHERE customer_name IS NOT NULL 
      AND customer_name != ''
    GROUP BY customer_name, company_name
    ORDER BY frequency DESC
    """
    
    cursor.execute(query)
    customers = cursor.fetchall()
    
    print(f"Found {len(customers)} unique customer name combinations")
    
    # Load explicit mappings
    explicit_mappings = load_customer_name_normalization_rules()
    
    # Analyze normalization patterns
    normalization_stats = defaultdict(int)
    interesting_changes = []
    
    for customer in customers:
        original = customer['customer_name']
        
        # Apply explicit mapping first, then automatic normalization
        if original in explicit_mappings:
            normalized = explicit_mappings[original]
            normalization_stats['explicit_mapping'] += 1
        else:
            normalized = normalize_customer_name(original)
            if normalized != original:
                normalization_stats['automatic_normalization'] += 1
            else:
                normalization_stats['no_change'] += 1
        
        # Track interesting changes for display
        if normalized != original:
            interesting_changes.append({
                'original': original,
                'normalized': normalized,
                'frequency': customer['frequency'],
                'type': 'explicit' if original in explicit_mappings else 'automatic'
            })
    
    # Show normalization results
    print("\nNormalization Summary:")
    print("-" * 30)
    for norm_type, count in normalization_stats.items():
        print(f"{norm_type}: {count}")
    
    # Show most interesting changes
    print(f"\nTop Customer Name Normalizations:")
    print("-" * 70)
    interesting_changes.sort(key=lambda x: x['frequency'], reverse=True)
    
    for change in interesting_changes[:20]:
        change_type = "E" if change['type'] == 'explicit' else "A"
        print(f"[{change_type}] {change['original']:40} → {change['normalized']:30} ({change['frequency']}x)")
    
    conn.close()
    return interesting_changes, normalization_stats

def create_customer_name_mapping_table():
    """
    Create a customer name mapping table in the database for DBT to use
    """
    print("\nCreating customer name mapping table for DBT...")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get all unique customer names
    query = """
    SELECT DISTINCT customer_name
    FROM raw.customers 
    WHERE customer_name IS NOT NULL 
      AND customer_name != ''
    """
    
    cursor.execute(query)
    customers = cursor.fetchall()
    
    # Load explicit mappings
    explicit_mappings = load_customer_name_normalization_rules()
    
    # Create mapping table
    cursor.execute("DROP TABLE IF EXISTS raw.customer_name_mapping")
    cursor.execute("""
        CREATE TABLE raw.customer_name_mapping (
            original_name VARCHAR(500) PRIMARY KEY,
            normalized_name VARCHAR(500) NOT NULL,
            normalization_type VARCHAR(50) NOT NULL,
            created_date TIMESTAMP DEFAULT NOW()
        )
    """)
    
    # Insert mappings
    explicit_count = 0
    automatic_count = 0
    no_change_count = 0
    
    for row in customers:
        original = row['customer_name']
        
        # Apply explicit mapping first, then automatic normalization
        if original in explicit_mappings:
            normalized = explicit_mappings[original]
            norm_type = 'explicit'
            explicit_count += 1
        else:
            normalized = normalize_customer_name(original)
            if normalized != original:
                norm_type = 'automatic'
                automatic_count += 1
            else:
                norm_type = 'no_change'
                no_change_count += 1
        
        cursor.execute("""
            INSERT INTO raw.customer_name_mapping (original_name, normalized_name, normalization_type)
            VALUES (%s, %s, %s)
            ON CONFLICT (original_name) DO UPDATE SET
                normalized_name = EXCLUDED.normalized_name,
                normalization_type = EXCLUDED.normalization_type,
                created_date = NOW()
        """, (original, normalized, norm_type))
    
    conn.commit()
    
    # Show summary
    print("Customer name mapping table created:")
    print(f"  explicit mappings: {explicit_count}")
    print(f"  automatic normalizations: {automatic_count}")
    print(f"  no changes: {no_change_count}")
    print(f"  total: {len(customers)}")
    
    conn.close()

if __name__ == "__main__":
    try:
        print("🔍 Starting Domain and Customer Name Analysis")
        print("=" * 60)
        
        # Phase 1: Domain Analysis
        domain_stats, normalization_mapping = analyze_domains()
        
        # Phase 2: Customer Name Analysis  
        customer_changes, customer_stats = analyze_customer_names()
        
        print("\n" + "="*60)
        print("Creating mapping tables for DBT...")
        
        # Create domain mapping table
        create_domain_mapping_table()
        
        # Create customer name mapping table
        create_customer_name_mapping_table()
        
        print("\n✅ Complete: Domain and Customer Name Analysis")
        print("\nSummary:")
        print(f"- Domain consolidations: {len(normalization_mapping)} → {len(domain_stats)}")
        print(f"- Customer name normalizations: {customer_stats.get('automatic_normalization', 0)} + {customer_stats.get('explicit_mapping', 0)}")
        print("\nMapping tables created:")
        print("- raw.domain_mapping (for company consolidation)")
        print("- raw.customer_name_mapping (for customer name standardization)")
        print("\nNext steps:")
        print("1. Review the consolidation results above")
        print("2. Update DBT models to use the new mapping tables")
        print("3. Test the complete pipeline")
        
    except Exception as e:
        print(f"❌ Error during analysis: {e}")
        raise