#!/usr/bin/env python3
import os
import sys
import re
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd

# Normalization functions
def normalize_company_name(name):
    """
    Normalize company name by:
    - Converting to lowercase
    - Truncating after common suffixes (LLC, Inc, Co, etc.)
    - Removing the suffixes themselves
    - Removing parenthetical information
    - Removing special characters and extra whitespace
    - Removing discount percentages and notes
    """
    if not name or not isinstance(name, str):
        return ""
    
    # Convert to lowercase
    name = name.lower()
    
    # FIRST PRIORITY: Truncate after common company suffixes
    # This is the most important step - if we find a suffix like LLC or Inc.,
    # we truncate everything after it, as that's usually the end of the company name
    suffix_patterns = [
        r'\bllc\b', 
        r'\binc\.?', 
        r'\bco\.?',
        r'\bltd\.?', 
        r'\bcorporation\b', 
        r'\bcorp\.?',
        r'\bcompany\b', 
        r'\bgroup\b',
        r'\bgmbh\b'
    ]
    
    # First, find any company suffix and truncate everything after it
    for pattern in suffix_patterns:
        match = re.search(pattern, name)
        if match:
            # Find the end of the suffix
            suffix_end = match.end()
            
            # Find the next space after the suffix
            next_space = name.find(' ', suffix_end)
            
            # If there's a space after the suffix, truncate there
            # Otherwise, keep the whole string (the suffix is at the end)
            if next_space > 0:
                name = name[:next_space]
            
            # Now remove the suffix itself
            name = re.sub(r'\b(llc|inc\.?|co\.?|ltd\.?|corporation|corp\.?|company|group|gmbh)\b', '', name)
            
            # Clean up any remaining special characters and whitespace
            name = re.sub(r'[^\w\s]', ' ', name)
            name = re.sub(r'\s+', ' ', name).strip()
            
            return name  # Return immediately after truncating at suffix and cleaning
    
    # If we get here, we didn't find a suffix, so try other methods of cleaning
    # Remove everything after common separators
    separators = [
        "end user", "see notes", "self storage", "use trucker", 
        "cc in notes", "anc", "epx", "tub", "grout", "epoxy",
        "per po", "note", "end", "user", "customer"
    ]
    
    for separator in separators:
        if separator in name:
            parts = name.split(separator, 1)
            name = parts[0]
    
    # Remove percentage patterns (e.g., "40%", "35%G", "40%A", "40%/35%")
    name = re.sub(r'\s*\d+%[A-Za-z]?(/\d+%[A-Za-z]?)?', '', name)
    
    # Remove parenthetical information
    name = re.sub(r'\s*\([^)]*\)\s*', ' ', name)
    
    # Now remove the suffixes themselves
    name = re.sub(r'\b(llc|inc\.?|co\.?|ltd\.?|corporation|corp\.?|company|group|gmbh)\b', '', name)
    
    # Remove special characters and normalize whitespace
    name = re.sub(r'[^\w\s]', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    
    return name

def advanced_normalize_company_name(name):
    """
    Apply more aggressive normalization techniques:
    - Apply basic normalization first
    - Remove common words like "the", "and", etc.
    - Handle common abbreviations
    - Remove digits and special formatting
    """
    if not name or not isinstance(name, str):
        return ""
    
    # Apply basic normalization first
    name = normalize_company_name(name)
    
    # Remove common words
    common_words = ['the', 'and', 'of', 'for', 'a', 'an', 'in', 'on', 'at', 'by']
    name_parts = name.split()
    name_parts = [part for part in name_parts if part.lower() not in common_words]
    name = ' '.join(name_parts)
    
    # Handle common abbreviations
    abbreviations = {
        'intl': 'international',
        'int': 'international',
        'natl': 'national',
        'nat': 'national',
        'dept': 'department',
        'univ': 'university',
        'assoc': 'associates',
        'assn': 'association',
        'mgmt': 'management',
        'svcs': 'services',
        'svc': 'service',
        'sys': 'systems',
        'tech': 'technology',
        'mfg': 'manufacturing',
        'dist': 'distribution',
        'ent': 'enterprises',
        'dev': 'development'
    }
    
    for abbr, full in abbreviations.items():
        name = re.sub(r'\b' + abbr + r'\b', full, name)
    
    # Remove digits and special formatting
    name = re.sub(r'\d+', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    
    return name

def get_acronym(name):
    """
    Extract acronym from a name (first letter of each word)
    """
    if not name or not isinstance(name, str):
        return ""
    
    words = name.split()
    if not words:
        return ""
    
    # Get first letter of each word
    acronym = ''.join(word[0] for word in words if word)
    return acronym.lower()

def handle_special_cases(name):
    """
    Handle special cases that we've identified in the data
    """
    if not name or not isinstance(name, str):
        return name
    
    # Convert to lowercase for easier matching
    name_lower = name.lower()
    
    # Special case mappings
    special_cases = {
        'lausd': 'los angeles unified school district',
        'la unified': 'los angeles unified school district',
        'la unified school district': 'los angeles unified school district',
        'los angeles unified': 'los angeles unified school district',
        'fedex': 'federal express',
        'ups': 'united parcel service',
        'usps': 'united states postal service',
        'us postal': 'united states postal service',
        'u.s. postal': 'united states postal service',
        'u.s.p.s': 'united states postal service',
        'u.p.s': 'united parcel service',
    }
    
    # Check if the name contains any of our special cases
    for key, value in special_cases.items():
        if key in name_lower:
            return value
    
    return name

def get_match_score(match_type, normalized_name1, normalized_name2):
    """
    Return a confidence score based on the match type and similarity
    """
    base_scores = {
        'exact': 1.0,
        'normalized_company_name': 0.9,
        'normalized_customer_name': 0.8,
        'normalized_billing_address': 0.7,
        'normalized_shipping_address': 0.6,
        'advanced_normalized': 0.5
    }
    
    base_score = base_scores.get(match_type, 0.0)
    
    # Add a bonus for longer name matches (more specific)
    length_bonus = 0
    if normalized_name1 and len(normalized_name1) > 3:
        length_bonus = min(0.1, len(normalized_name1) / 100)
    
    return base_score + length_bonus

def create_company_order_mapping_table(connection, matches_df, first_batch=False):
    """
    Create a company_order_mapping table in the database
    """
    # Create the table if it doesn't exist
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS public.company_order_mapping (
        quickbooks_id VARCHAR(255) NOT NULL,
        company_id VARCHAR(255) NOT NULL,
        order_number VARCHAR(255),
        match_type VARCHAR(50),
        confidence FLOAT,
        original_customer_name TEXT,
        original_company_name TEXT,
        normalized_customer_name TEXT,
        normalized_company_name TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (quickbooks_id, company_id)
    )
    """
    
    connection.execute(text(create_table_sql))
    connection.commit()  # Explicitly commit the transaction
    
    # Prepare data for insertion
    records = []
    for _, match in matches_df.iterrows():
        record = {
            'quickbooks_id': match['quickbooks_id'],
            'company_id': match['company_id'],
            'order_number': match['order_number'],
            'match_type': match['match_type'],
            'confidence': match['confidence'],
            'original_customer_name': match['customer_name'],
            'original_company_name': match['company_name'],
            'normalized_customer_name': match.get('normalized_customer_name', ''),
            'normalized_company_name': match['normalized_company_name']
        }
        records.append(record)
    
    # Clear existing data only for the first batch
    if first_batch:
        print("Truncating existing company_order_mapping table...")
        connection.execute(text("TRUNCATE TABLE public.company_order_mapping"))
        connection.commit()  # Explicitly commit the transaction
    
    # Insert new data
    if records:
        # Convert to DataFrame for easy insertion
        insert_df = pd.DataFrame(records)
        
        # Use pandas to_sql for bulk insertion
        insert_df.to_sql('company_order_mapping', 
                         connection, 
                         schema='public', 
                         if_exists='append', 
                         index=False,
                         method='multi')
        
        # Only print this message for the first batch and the last batch
        if first_batch or len(records) < 100:  # Assuming last batch is smaller
            print(f"Successfully inserted {len(records)} records into company_order_mapping table")
        
        try:
            # Create a view for easy querying
            create_view_sql = """
            DROP VIEW IF EXISTS public.order_company_view;
            CREATE VIEW public.order_company_view AS
            SELECT 
                o.quickbooks_id,
                o.order_number,
                o.customer_name,
                o.order_date,
                o.total_amount,
                o.billing_address_line_1,
                o.shipping_address_line_1,
                c.company_id,
                c.company_name,
                c.company_domain,
                m.match_type,
                m.confidence
            FROM 
                public.orders o
            JOIN 
                public.company_order_mapping m ON o.quickbooks_id = m.quickbooks_id
            JOIN 
                public.companies c ON m.company_id = c.company_id
            ORDER BY 
                o.order_date DESC
            """
            
            # Execute as separate statements
            connection.execute(text("DROP VIEW IF EXISTS public.order_company_view"))
            connection.commit()  # Commit after dropping view
            
            connection.execute(text("""
            CREATE VIEW public.order_company_view AS
            SELECT 
                o.quickbooks_id,
                o.order_number,
                o.customer_name,
                o.order_date,
                o.total_amount,
                o.billing_address_line_1,
                o.shipping_address_line_1,
                c.company_id,
                c.company_name,
                c.company_domain,
                m.match_type,
                m.confidence
            FROM 
                public.orders o
            JOIN 
                public.company_order_mapping m ON o.quickbooks_id = m.quickbooks_id
            JOIN 
                public.companies c ON m.company_id = c.company_id
            ORDER BY 
                o.order_date DESC
            """))
            connection.commit()  # Commit after creating view
            
            # Verify the view was created
            view_check = connection.execute(text("""
            SELECT EXISTS (
                SELECT 1 
                FROM information_schema.views 
                WHERE table_schema = 'public' 
                AND table_name = 'order_company_view'
            )
            """))
            view_exists = view_check.scalar()
            
            if view_exists:
                # Check if there are records in the view
                count_check = connection.execute(text("SELECT COUNT(*) FROM public.order_company_view"))
                record_count = count_check.scalar()
                print(f"The view contains {record_count} records")
            else:
                print("WARNING: Failed to create order_company_view")
        except Exception as e:
            print(f"Error creating view: {e}")

def main():
    # Load environment variables from .env or .env.local
    if os.path.exists('.env.local'):
        load_dotenv('.env.local')
    else:
        load_dotenv()
    
    # Get database connection details
    db_user = 'aac'
    db_password = os.getenv('DBT_POSTGRES_PASSWORD')
    db_host = 'localhost'
    db_port = '5432'
    db_name = 'mqi'
    
    # Create SQLAlchemy engine
    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(connection_string)
    
    try:
        # Connect to the database and load data
        with engine.connect() as connection:
            # Get company data
            companies_query = """
            SELECT company_id, company_name, company_domain 
            FROM public.companies
            """
            companies_df = pd.read_sql(companies_query, connection)
            
            # Set to True for testing with a sample, False for processing all orders
            sample_mode = False
            
            # Set to True to save results to database
            save_to_db = True
            
            # Get total order count
            count_query = "SELECT COUNT(*) FROM public.orders"
            total_orders = pd.read_sql(count_query, connection).iloc[0, 0]
            
            # Process in batches to improve performance
            batch_size = 500
            total_batches = (total_orders + batch_size - 1) // batch_size
            
            print(f"Total companies: {len(companies_df)}")
            print(f"Total orders: {total_orders}")
            print(f"Processing in batches of {batch_size}")
            
            # Add normalized columns to companies (only need to do this once)
            print("Normalizing company names...")
            companies_df['normalized_company_name'] = companies_df['company_name'].apply(normalize_company_name)
            
            # Create dictionaries for faster lookups
            print("Creating lookup dictionaries...")
            company_name_dict = {}
            company_acronym_dict = {}
            
            for _, company in companies_df.iterrows():
                # Add to normalized name dictionary
                norm_name = company['normalized_company_name']
                if norm_name not in company_name_dict:
                    company_name_dict[norm_name] = []
                company_name_dict[norm_name].append({
                    'company_id': company['company_id'],
                    'company_name': company['company_name'],
                    'normalized_company_name': norm_name
                })
                
                # Add to acronym dictionary
                acronym = get_acronym(company['company_name'])
                if acronym and len(acronym) >= 2:
                    if acronym not in company_acronym_dict:
                        company_acronym_dict[acronym] = []
                    company_acronym_dict[acronym].append({
                        'company_id': company['company_id'],
                        'company_name': company['company_name'],
                        'normalized_company_name': company['normalized_company_name']
                    })
            
            # Process orders in batches
            all_matches = []
            processed_count = 0
            
            for batch_num in range(total_batches):
                offset = batch_num * batch_size
                
                # Get batch of orders
                orders_query = f"""
                SELECT quickbooks_id, customer_name, billing_address_line_1, 
                       shipping_address_line_1, order_number
                FROM public.orders
                ORDER BY quickbooks_id
                LIMIT {batch_size} OFFSET {offset}
                """
                orders_df = pd.read_sql(orders_query, connection)
                
                if orders_df.empty:
                    break
                
                # Only print batch progress every 5 batches to reduce output
                if batch_num % 5 == 0 or batch_num == total_batches - 1:
                    print(f"\nProcessing batch {batch_num + 1}/{total_batches} ({offset+1}-{offset+len(orders_df)} of {total_orders})")
                
                # Normalize order fields
                orders_df['normalized_customer_name'] = orders_df['customer_name'].apply(normalize_company_name)
                orders_df['normalized_billing_address'] = orders_df['billing_address_line_1'].apply(normalize_company_name)
                orders_df['normalized_shipping_address'] = orders_df['shipping_address_line_1'].apply(normalize_company_name)
                
                # Find matches for this batch
                batch_matches = []
                
                # For each order in the batch, try to find matches
                for idx, order in orders_df.iterrows():
                    # Removed the per-100 progress updates to reduce output verbosity
                    
                    order_matches = []
                    
                    # Try matching on normalized customer name
                    if order['normalized_customer_name']:
                        # Use dictionary lookup instead of DataFrame filtering for better performance
                        norm_name = order['normalized_customer_name']
                        if norm_name in company_name_dict:
                            for company in company_name_dict[norm_name]:
                                order_matches.append({
                                    'quickbooks_id': order['quickbooks_id'],
                                    'order_number': order['order_number'],
                                    'customer_name': order['customer_name'],
                                    'normalized_customer_name': norm_name,
                                    'company_id': company['company_id'],
                                    'company_name': company['company_name'],
                                    'normalized_company_name': company['normalized_company_name'],
                                    'match_type': 'normalized_customer_name',
                                    'confidence': 0.8  # Fixed confidence for better performance
                                })
                    
                    # Try matching on normalized billing address if no customer name match
                    if not order_matches and order['normalized_billing_address']:
                        norm_billing = order['normalized_billing_address']
                        if norm_billing in company_name_dict:
                            for company in company_name_dict[norm_billing]:
                                order_matches.append({
                                    'quickbooks_id': order['quickbooks_id'],
                                    'order_number': order['order_number'],
                                    'customer_name': order['customer_name'],
                                    'normalized_customer_name': norm_billing,
                                    'company_id': company['company_id'],
                                    'company_name': company['company_name'],
                                    'normalized_company_name': company['normalized_company_name'],
                                    'match_type': 'normalized_billing_address',
                                    'confidence': 0.7  # Fixed confidence for better performance
                                })
                    
                    # Try matching on normalized shipping address if no other matches
                    if not order_matches and order['normalized_shipping_address']:
                        norm_shipping = order['normalized_shipping_address']
                        if norm_shipping in company_name_dict:
                            for company in company_name_dict[norm_shipping]:
                                order_matches.append({
                                    'quickbooks_id': order['quickbooks_id'],
                                    'order_number': order['order_number'],
                                    'customer_name': order['customer_name'],
                                    'normalized_customer_name': norm_shipping,
                                    'company_id': company['company_id'],
                                    'company_name': company['company_name'],
                                    'normalized_company_name': company['normalized_company_name'],
                                    'match_type': 'normalized_shipping_address',
                                    'confidence': 0.6  # Fixed confidence for better performance
                                })
                    
                    # Try special case handling for remaining unmatched orders
                    if not order_matches:
                        # Apply special case handling to customer name
                        special_customer = handle_special_cases(order['customer_name'])
                        if special_customer and special_customer != order['customer_name']:
                            special_customer_norm = normalize_company_name(special_customer)
                            if special_customer_norm in company_name_dict:
                                for company in company_name_dict[special_customer_norm]:
                                    order_matches.append({
                                        'quickbooks_id': order['quickbooks_id'],
                                        'order_number': order['order_number'],
                                        'customer_name': order['customer_name'],
                                        'normalized_customer_name': special_customer_norm,
                                        'company_id': company['company_id'],
                                        'company_name': company['company_name'],
                                        'normalized_company_name': company['normalized_company_name'],
                                        'match_type': 'special_case',
                                        'confidence': 0.75  # Slightly lower confidence for special cases
                                    })
                    
                    # Try acronym matching for remaining unmatched orders - MUCH more restrictive
                    if not order_matches:
                        # Only try acronym matching if the customer name looks like a company (not a person)
                        customer_name = order['customer_name']
                        
                        # Skip if it looks like a person's name (First Last format)
                        if re.match(r'^[A-Z][a-z]+ [A-Z][a-z]+$', customer_name):
                            continue
                            
                        # Skip if it contains common name patterns
                        if re.search(r'(,\s*[A-Z][a-z]+$)|(^[A-Z][a-z]+\s+[A-Z]\.?$)', customer_name):
                            continue
                        
                        # Get acronym from customer name
                        customer_acronym = get_acronym(customer_name)
                        
                        # Only use acronyms with 3+ chars and that don't match common patterns
                        if (customer_acronym and len(customer_acronym) >= 3 and 
                            not re.match(r'^(mr|mrs|ms|dr|jr|sr|inc|llc|co|ltd)$', customer_acronym)):
                            
                            # Use dictionary lookup for acronyms
                            if customer_acronym in company_acronym_dict:
                                # Additional validation - only match if the acronym is unique enough
                                matches = company_acronym_dict[customer_acronym]
                                if len(matches) <= 2:  # Only match if there are at most 2 companies with this acronym
                                    for company in matches:
                                        order_matches.append({
                                            'quickbooks_id': order['quickbooks_id'],
                                            'order_number': order['order_number'],
                                            'customer_name': order['customer_name'],
                                            'normalized_customer_name': customer_acronym,
                                            'company_id': company['company_id'],
                                            'company_name': company['company_name'],
                                            'normalized_company_name': company['normalized_company_name'],
                                            'match_type': 'acronym_match',
                                            'confidence': 0.5  # Even lower confidence for acronym matches
                                        })
                    
                    # If we found matches for this order, add the best one to our results
                    if order_matches:
                        # Sort by confidence score (highest first)
                        order_matches.sort(key=lambda x: x['confidence'], reverse=True)
                        # Add the best match to our results
                        batch_matches.append(order_matches[0])
                
                # Add batch matches to all matches
                all_matches.extend(batch_matches)
                
                # Update processed count and batch stats
                processed_count += len(orders_df)
                matched_count = len(batch_matches)
                batch_match_rate = matched_count / len(orders_df) * 100
                
                # Only print batch progress every 5 batches to reduce output
                if batch_num % 5 == 0 or batch_num == total_batches - 1:
                    print(f"  Batch {batch_num + 1}: {matched_count}/{len(orders_df)} orders matched ({batch_match_rate:.2f}%)")
                
                # Save batch results to database if enabled
                if save_to_db and batch_matches:
                    batch_df = pd.DataFrame(batch_matches)
                    # Only print saving message every 5 batches to reduce output
                    if batch_num % 5 == 0 or batch_num == total_batches - 1:
                        print(f"  Saving batch {batch_num + 1} results to database...")
                    create_company_order_mapping_table(connection, batch_df, first_batch=(batch_num == 0))
                
                # Stop after first batch if in sample mode
                if sample_mode and batch_num == 0:
                    print("\nSample mode: stopping after first batch")
                    break
            
            # Print final stats
            if all_matches:
                matches_df = pd.DataFrame(all_matches)
                
                # Calculate overall match rate
                total_processed = processed_count
                total_matched = len(matches_df)
                overall_match_rate = total_matched / total_processed * 100
                
                # Print a summary box for better visibility
                print("\n" + "="*80)
                print("MATCHING SUMMARY")
                print("="*80)
                print(f"Total orders processed:  {total_processed}")
                print(f"Total matches found:     {total_matched}")
                print(f"Overall match rate:      {overall_match_rate:.2f}%")
                print("-"*80)
                
                # Print match type distribution
                match_counts = matches_df['match_type'].value_counts()
                print("Match type distribution:")
                for match_type, count in match_counts.items():
                    percentage = count / total_matched * 100
                    print(f"  {match_type:<25} {count:>5} ({percentage:.2f}%)")
                print("-"*80)
                
                # Print sample matches
                print("\nSample matches:")
                sample_df = matches_df.head(5)[['customer_name', 'company_name', 'match_type', 'confidence']]
                for _, row in sample_df.iterrows():
                    print(f"  {row['customer_name'][:30]:<30} → {row['company_name'][:30]:<30} ({row['match_type']}, {row['confidence']:.2f})")
                print("="*80)
                
                # Create the view for easy querying
                if save_to_db:
                    print("\nCreating order_company_view...")
                    create_view_sql = """
                    CREATE OR REPLACE VIEW public.order_company_view AS
                    SELECT 
                        o.quickbooks_id,
                        o.order_number,
                        o.customer_name,
                        o.order_date,
                        o.total_amount,
                        o.billing_address_line_1,
                        o.shipping_address_line_1,
                        c.company_id,
                        c.company_name,
                        c.company_domain,
                        m.match_type,
                        m.confidence
                    FROM 
                        public.orders o
                    JOIN 
                        public.company_order_mapping m ON o.quickbooks_id = m.quickbooks_id
                    JOIN 
                        public.companies c ON m.company_id = c.company_id
                    ORDER BY 
                        o.order_date DESC
                    """
                    connection.execute(text(create_view_sql))
                    connection.commit()  # Commit after creating view
            else:
                print("\nNo matches found")
            
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
