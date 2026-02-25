"""
SFR Listings Scraper
Scrapes single-family rental listings from 6 providers into one database.
Sources: Tricon, Progress Residential, Invitation Homes, AMH, Main Street Renewal, FirstKey Homes
"""

import json
import math
import os
import re
import traceback
from datetime import datetime

import cloudscraper
import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, inspect

load_dotenv()

# ── Config ───────────────────────────────────────────────────────────────────

DATABASE_URL = os.environ.get('DATABASE_URL', '')
if DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

JSON_DIR = 'json'

BROWSER_UA = (
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/144.0.0.0 Safari/537.36'
)

STATE_FULL_NAMES = {
    'AL': 'alabama', 'AZ': 'arizona', 'AR': 'arkansas', 'CA': 'california',
    'CO': 'colorado', 'CT': 'connecticut', 'FL': 'florida', 'GA': 'georgia',
    'IL': 'illinois', 'IN': 'indiana', 'KY': 'kentucky', 'LA': 'louisiana',
    'MD': 'maryland', 'MI': 'michigan', 'MN': 'minnesota', 'MO': 'missouri',
    'MS': 'mississippi', 'NC': 'north-carolina', 'NV': 'nevada',
    'NJ': 'new-jersey', 'NY': 'new-york', 'OH': 'ohio', 'OK': 'oklahoma',
    'OR': 'oregon', 'PA': 'pennsylvania', 'SC': 'south-carolina',
    'TN': 'tennessee', 'TX': 'texas', 'UT': 'utah', 'VA': 'virginia',
    'WA': 'washington', 'WI': 'wisconsin',
}

INVH_MARKETS = {
    'atlanta-georgia': (33.748995, -84.387982),
    'austin-texas': (30.266666, -97.73333),
    'charlotte-north-carolina': (35.521445, -79.905591),
    'chicago-illinois': (41.878114, -87.629798),
    'dallas-texas': (32.776664, -96.796988),
    'denver-colorado': (39.739236, -104.990251),
    'houston-texas': (29.760427, -95.369803),
    'jacksonville-florida': (30.332184, -81.655651),
    'las-vegas-nevada': (36.169941, -115.13983),
    'los-angeles-california': (34.052234, -118.243685),
    'miami-florida': (25.76168, -80.19179),
    'minneapolis-minnesota': (44.977753, -93.265011),
    'nashville-tennessee': (36.162664, -86.781602),
    'orlando-florida': (28.538335, -81.379236),
    'phoenix-arizona': (33.448377, -112.074037),
    'sacramento-california': (38.581572, -121.4944),
    'salt-lake-city-utah': (40.7608, -111.891),
    'san-antonio-texas': (29.424349, -98.491142),
    'seattle-washington': (47.606209, -122.332071),
    'tampa-florida': (27.950575, -82.457178),
}

FKH_MARKETS = [
    'atlanta', 'birmingham', 'charleston', 'charlotte', 'chicago',
    'cincinnati', 'colorado-springs', 'columbus', 'dallas', 'denver',
    'fort-worth', 'ft-myers', 'greenville', 'houston', 'indianapolis',
    'jacksonville', 'kansas-city', 'las-vegas', 'louisville', 'memphis',
    'miami', 'nashville', 'oklahoma-city', 'orlando', 'overland-park',
    'phoenix', 'raleigh-durham', 'san-antonio', 'st-louis', 'tampa',
    'tucson', 'winston-salem',
]

SCRAPERS = {
    '1': ('Tricon Residential', 'tricon'),
    '2': ('Progress Residential', 'progress'),
    '3': ('Invitation Homes', 'invh'),
    '4': ('AMH', 'amh'),
    '5': ('Main Street Renewal', 'msr'),
    '6': ('FirstKey Homes', 'firstkey'),
}

TABLE_NAMES = {
    'tricon': 'tricon_listings',
    'progress': 'progress_listings',
    'invh': 'invh_listings',
    'amh': 'amh_listings',
    'msr': 'msr_listings',
    'firstkey': 'firstkey_listings',
}

UNIQUE_KEYS = {
    'tricon': 'unit_code',
    'progress': 'property_id',
    'invh': 'property_id',
    'amh': 'property_id',
    'msr': '_id',
    'firstkey': 'property_id',
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def save_json(data, filename):
    """Save cleaned JSON to the json/ directory."""
    os.makedirs(JSON_DIR, exist_ok=True)
    path = os.path.join(JSON_DIR, filename)
    with open(path, 'w') as f:
        json.dump(data, f, indent=2, default=str)
    print(f'  Saved JSON -> {path}')


def get_engine():
    """Create a SQLAlchemy engine from DATABASE_URL."""
    if not DATABASE_URL:
        raise RuntimeError('DATABASE_URL not set. Add it to .env or environment.')
    return create_engine(DATABASE_URL)


def upsert_to_db(df, table_name, unique_key):
    """UPSERT DataFrame into a Postgres table with soft-delete tracking."""
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    engine = get_engine()

    # Check if the table exists
    inspector = inspect(engine)
    table_exists = table_name in inspector.get_table_names()

    if table_exists:
        existing = pd.read_sql(f'SELECT * FROM "{table_name}"', engine)

        # Mark all existing as inactive (pre-scrape)
        existing['is_active'] = False

        # Build lookup of existing rows by unique key
        existing_keys = set(existing[unique_key].astype(str))

        # Prepare new data
        df = df.copy()
        df[unique_key] = df[unique_key].astype(str)

        new_keys = set(df[unique_key])
        updated_keys = existing_keys & new_keys
        inserted_keys = new_keys - existing_keys
        deactivated_keys = existing_keys - new_keys

        # Build the merged DataFrame
        # Start with existing rows that are NOT in the new scrape (deactivated)
        deactivated = existing[existing[unique_key].astype(str).isin(deactivated_keys)].copy()
        # is_active already False, keep first_seen and last_seen as-is

        # For updated rows: take new data but preserve first_seen
        updated_rows = []
        for _, row in df[df[unique_key].isin(updated_keys)].iterrows():
            old_row = existing[existing[unique_key].astype(str) == str(row[unique_key])].iloc[0]
            row_dict = row.to_dict()
            row_dict['is_active'] = True
            row_dict['first_seen'] = old_row.get('first_seen', now)
            row_dict['last_seen'] = now
            updated_rows.append(row_dict)

        # For new rows
        new_rows = []
        for _, row in df[df[unique_key].isin(inserted_keys)].iterrows():
            row_dict = row.to_dict()
            row_dict['is_active'] = True
            row_dict['first_seen'] = now
            row_dict['last_seen'] = now
            new_rows.append(row_dict)

        # Combine all
        parts = [deactivated]
        if updated_rows:
            parts.append(pd.DataFrame(updated_rows))
        if new_rows:
            parts.append(pd.DataFrame(new_rows))

        merged = pd.concat(parts, ignore_index=True)

        print(f'  {len(inserted_keys)} new, {len(updated_keys)} updated, {len(deactivated_keys)} deactivated')
    else:
        # First run — all rows are new
        df = df.copy()
        df['is_active'] = True
        df['first_seen'] = now
        df['last_seen'] = now
        merged = df
        print(f'  {len(df)} new (first run)')

    merged.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f'  Wrote {len(merged)} total rows -> {table_name}')
    engine.dispose()


def dedup_df(df, subset=None):
    """Remove duplicates and report count."""
    before = len(df)
    df = df.drop_duplicates(subset=subset, keep='first')
    dropped = before - len(df)
    if dropped:
        print(f'  Dropped {dropped} duplicate rows')
    return df


# ── Scrapers ─────────────────────────────────────────────────────────────────

def scrape_tricon(metro_loc):
    """Scrape Tricon Residential listings."""
    print('Scraping Tricon Residential...')
    headers = {
        'sec-ch-ua': '"Chromium";v="112", "Google Chrome";v="112", "Not:A-Brand";v="99"',
        'Accept': 'application/json, text/plain, */*',
        'Referer': 'https://triconresidential.com/',
        'User-Agent': BROWSER_UA,
    }

    all_pages = []
    records = []
    page, last_page = 1, 999

    while page <= last_page:
        url = f'https://triconresidential.com/static/regions/{metro_loc}/{page}.json'
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        results = response.json()
        all_pages.append(results)
        last_page = results['meta']['last_page']

        for item in results['data']:
            availability = item.get('availability', {})
            records.append({
                'address': item.get('title', ''),
                'bed': item.get('beds', ''),
                'bath': item.get('baths', ''),
                'square_feet': item.get('square_feet', ''),
                'city': item.get('city', ''),
                'state': item.get('state', ''),
                'zip_code': item.get('zip', ''),
                'region_id': item.get('region_id', ''),
                'available': availability.get('display', ''),
                'price': item.get('rent', ''),
                'min_rent': item.get('min_rent', ''),
                'max_rent': item.get('max_rent', ''),
                'special': item.get('special', ''),
                'link': f"https://triconresidential.com/home/{item.get('slug', '')}",
                'self_tour_link': item.get('self_tour_url', ''),
                'virtual_tour_link': item.get('virtual_tour_url', ''),
                'unit_code': item.get('unit_code', ''),
                'metro_location': metro_loc,
            })
        print(f'  Page {page}/{last_page} -- {len(results["data"])} listings')
        page += 1

    save_json(all_pages, f'tricon_{metro_loc}.json')
    df = dedup_df(pd.DataFrame(records), subset='unit_code')
    print(f'  Total: {len(df)} listings')
    return df


def scrape_progress(metro_loc):
    """Scrape Progress Residential listings (uses cloudscraper for Cloudflare)."""
    print('Scraping Progress Residential...')
    scraper = cloudscraper.create_scraper()
    rows_per_page = 100

    all_pages = []
    records = []
    page, last_page = 1, 999

    while page <= last_page:
        url = (
            f'https://rentprogress.com/bin/progress-residential/'
            f'property-search.market-{metro_loc}.page-{page}'
            f'.rows-{rows_per_page}.nr-{rows_per_page}.json'
        )
        response = scraper.get(url)
        response.raise_for_status()
        data = response.json()
        all_pages.append(data)

        total = data['recordsFound']
        last_page = math.ceil(total / rows_per_page)

        for item in data['results']:
            location = item.get('location', {})
            records.append({
                'property_id': item.get('propertyId', ''),
                'street': item.get('street', ''),
                'city': item.get('city', ''),
                'state': item.get('state', ''),
                'zip': item.get('zip', ''),
                'beds': item.get('beds', ''),
                'baths': item.get('baths', ''),
                'sqft': item.get('sqft', ''),
                'year_built': item.get('yearBuilt', ''),
                'current_price': item.get('currentPrice', ''),
                'old_price': item.get('oldPrice', ''),
                'price_drop': item.get('priceDrop', False),
                'date_available': item.get('dateAvailable', ''),
                'property_status': item.get('propertyStatus', ''),
                'banner_status': item.get('bannerStatus', ''),
                'market': item.get('market', ''),
                'smart_home': item.get('smartHome', False),
                'solar_panels': item.get('solarPanels', False),
                'community_name': item.get('communityName', ''),
                'lat': location.get('lat', ''),
                'lng': location.get('lng', ''),
                'link': 'https://rentprogress.com' + item.get('pageUrl', ''),
                'thumbnail': item.get('thumbnailImage', ''),
                'metro_location': metro_loc,
            })
        print(f'  Page {page}/{last_page} -- {len(data["results"])} listings (total: {total})')
        page += 1

    save_json(all_pages, f'progress_{metro_loc}.json')
    df = dedup_df(pd.DataFrame(records), subset='property_id')
    print(f'  Total: {len(df)} listings')
    return df


def scrape_invh(metro_loc, lat, lng):
    """Scrape Invitation Homes listings via geo-search API."""
    print('Scraping Invitation Homes...')
    headers = {
        'accept': 'application/json',
        'user-agent': BROWSER_UA,
        'referer': 'https://www.invitationhomes.com/search/houses-for-rent',
    }

    LIMIT = 25
    bbox_pad = 0.65
    all_pages = []
    records = []
    offset, total = 0, 999

    while offset < total:
        url = (
            f'https://www.invitationhomes.com/property/api/geo-search'
            f'?baths_min=1&beds_min=1'
            f'&rent_min=0&rent_max=10000'
            f'&sqft_min=0&sqft_max=10000'
            f'&south={lat - bbox_pad}&west={lng - bbox_pad}'
            f'&north={lat + bbox_pad}&east={lng + bbox_pad}'
            f'&lat={lat}&long={lng}'
            f'&limit={LIMIT}&offset={offset}'
            f'&sort=distance&sort_direction=asc'
        )
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        all_pages.append(data)

        if not data.get('properties'):
            break
        total = data['total']

        for item in data['properties']:
            addr = item.get('address', {})
            loc = item.get('map_location', {})
            records.append({
                'property_id': item.get('property_id', ''),
                'street': addr.get('address_1', ''),
                'city': addr.get('city', ''),
                'state': addr.get('state', ''),
                'zip': addr.get('zip_code', ''),
                'beds': item.get('beds', ''),
                'baths': item.get('baths', ''),
                'sqft': item.get('square_footage', ''),
                'rent': item.get('rent', ''),
                'total_monthly_rent': item.get('total_monthly_rent', ''),
                'status': item.get('status', ''),
                'available_on': item.get('available_on', ''),
                'property_type': item.get('property_type', ''),
                'is_on_special': item.get('is_on_special', False),
                'is_self_show_enabled': item.get('is_self_show_enabled', False),
                'has_virtual_tour': item.get('has_virtual_tour', False),
                'is_new_construction': item.get('is_new_construction', False),
                'community': item.get('community', {}).get('name', '') if isinstance(item.get('community'), dict) else (item.get('community') or ''),
                'market_name': item.get('market_name', ''),
                'lat': loc.get('latitude', ''),
                'lng': loc.get('longitude', ''),
                'link': 'https://www.invitationhomes.com/houses-for-rent/' + item.get('slug', ''),
                'metro_location': metro_loc,
            })
        print(f'  Offset {offset}/{total} -- {len(data["properties"])} listings')
        offset += LIMIT

    save_json(all_pages, f'invh_{metro_loc}.json')
    df = dedup_df(pd.DataFrame(records), subset='property_id')
    print(f'  Total: {len(df)} listings')
    return df


def scrape_amh(metro_loc):
    """Scrape AMH (American Homes 4 Rent) listings via Next.js data endpoint."""
    print('Scraping AMH...')
    headers = {
        'user-agent': BROWSER_UA,
        'accept': 'application/json',
        'x-nextjs-data': '1',
    }

    # Fetch the current buildId (changes on each deploy)
    page_resp = requests.get('https://www.amh.com/', headers={'user-agent': BROWSER_UA})
    match = re.search(r'"buildId":"([^"]+)"', page_resp.text)
    if not match:
        raise RuntimeError('Could not find AMH buildId. Site may have changed.')
    build_id = match.group(1)
    print(f'  buildId: {build_id}')

    url = f'https://www.amh.com/_next/data/{build_id}/query.json?criteria={metro_loc}'
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    data = response.json()['pageProps']
    results = data.get('results', [])
    save_json(results, f'amh_{metro_loc}.json')

    records = []
    for item in results:
        address = item.get('addressLine1', '')
        state = item.get('state', '')
        zip_code = item.get('zipCode', '')
        property_no = item.get('propertyNo', '')

        if address and state and zip_code and property_no:
            link = (
                f"https://www.amh.com/p/{state.lower()}/"
                f"{address.replace(' ', '-')}-{state.lower()}-{zip_code}--{property_no}"
            )
        else:
            link = ''

        records.append({
            'property_id': property_no,
            'street': address,
            'city': item.get('city', ''),
            'state': state,
            'zip': zip_code,
            'beds': item.get('bedrooms', ''),
            'baths': item.get('bathrooms', ''),
            'sqft': item.get('unitSqFt', ''),
            'year_built': item.get('yearBuilt', ''),
            'rent': item.get('rent', ''),
            'total_rent': item.get('totalRent', ''),
            'available_date': item.get('availableDate', ''),
            'virtual_tour_url': item.get('virtualTourUrl', ''),
            'can_self_tour': item.get('canLetYourselfIn', False),
            'can_apply': item.get('canApply', False),
            'lat': item.get('latitude', ''),
            'lng': item.get('longitude', ''),
            'link': link,
            'metro_location': metro_loc,
        })

    df = dedup_df(pd.DataFrame(records), subset='property_id')
    print(f'  Total: {len(df)} listings')
    return df


def scrape_msr(market_name, state_code, metro_loc):
    """Scrape Main Street Renewal listings via Elasticsearch."""
    print('Scraping Main Street Renewal...')
    ES_URL = 'https://leasing-prod.es.us-east-1.aws.found.io/listings-v2/_search'
    headers = {
        'accept': 'application/json',
        'authorization': 'apiKey Z3JqdWNKRUJCaElILXdxR1lNS2Q6Z3hTc0RQQkVUSVNOcWZ6QVRUOTVHQQ==',
        'content-type': 'application/json',
        'origin': 'https://www.msrenewal.com',
        'referer': 'https://www.msrenewal.com/',
        'user-agent': BROWSER_UA,
    }

    query = {
        'from': 0,
        'size': 1500,
        'query': {
            'bool': {
                'must': [
                    {'match': {'Record_Type_Text__c': 'Leasing'}},
                    {'term': {'Syndicate_MSR__c': {'value': True}}},
                    {'bool': {
                        'should': [
                            {'match': {'Listing_Status__c': 'Active'}},
                            {'match': {'Listing_Status__c': 'Coming Soon'}},
                            {'match': {'Listing_Status__c': 'In-Repair'}},
                        ],
                        'must_not': {'term': {'Hide_Listing_for_Pending_Execution__c': {'value': True}}},
                    }},
                    {'match': {'Market__r.Name': market_name}},
                    {'match': {'Market__r.State__c': state_code}},
                ]
            }
        },
        '_source': [
            'Name', 'Listing_Status__c', 'Premium_Listing__c', 'Hot_Home__c',
            'Market__r.Name', 'Market__r.State__c',
            'Property__r.Name', 'Property__r.City__c', 'Property__r.State_Code__c',
            'Property__r.Zipcode__c', 'Property__r.Beds__c', 'Property__r.Baths__c',
            'Property__r.Square_Ft__c', 'Property__r.Rent__c', 'Property__r.Year_Built__c',
            'Property__r.Available_Date__c', 'Property__r.Latitude__c', 'Property__r.Longitude__c',
            'Property__r.Zillow_3d_Link__c',
            'Property__r.Specials__r.Marketing_Description__c',
        ],
    }

    response = requests.post(ES_URL, json=query, headers=headers)
    response.raise_for_status()

    data = response.json()
    hits = data['hits']['hits']
    total = data['hits']['total']['value']
    save_json(hits, f'msr_{metro_loc}.json')

    records = []
    for hit in hits:
        s = hit['_source']
        prop = s.get('Property__r', {})
        specials = prop.get('Specials__r', {})
        if isinstance(specials, list) and specials:
            special_text = specials[0].get('Marketing_Description__c', '')
        elif isinstance(specials, dict):
            special_text = specials.get('Marketing_Description__c', '')
        else:
            special_text = ''

        listing_name = s.get('Name', '')
        records.append({
            '_id': hit.get('_id', ''),
            'name': listing_name,
            'street': prop.get('Name', ''),
            'city': prop.get('City__c', ''),
            'state': prop.get('State_Code__c', ''),
            'zip': prop.get('Zipcode__c', ''),
            'beds': prop.get('Beds__c', ''),
            'baths': prop.get('Baths__c', ''),
            'sqft': prop.get('Square_Ft__c', ''),
            'rent': prop.get('Rent__c', ''),
            'year_built': prop.get('Year_Built__c', ''),
            'available_date': prop.get('Available_Date__c', ''),
            'listing_status': s.get('Listing_Status__c', ''),
            'premium': s.get('Premium_Listing__c', False),
            'hot_home': s.get('Hot_Home__c', False),
            'special': special_text,
            'lat': prop.get('Latitude__c', ''),
            'lng': prop.get('Longitude__c', ''),
            'virtual_tour': prop.get('Zillow_3d_Link__c', ''),
            'metro_location': metro_loc,
            'link': (
                'https://www.msrenewal.com/home/'
                + listing_name.replace(' ', '-').replace(',', '')
                + '/' + hit.get('_id', '')
            ),
        })

    df = dedup_df(pd.DataFrame(records), subset='_id')
    print(f'  Total: {total} found, {len(df)} scraped')
    return df


def scrape_firstkey(metro_loc):
    """Scrape FirstKey Homes listings from server-side rendered HTML."""
    print('Scraping FirstKey Homes...')
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'accept-language': 'en-US,en;q=0.9',
        'user-agent': BROWSER_UA,
    }

    url = 'https://www.firstkeyhomes.com/rental-homes/' + metro_loc
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    html = response.text
    print(f'  Page size: {len(html):,} bytes')

    # The HTML uses \&q; as escaped quote characters in embedded S2A data
    marker = '{\\&q;_jsonit\\&q;:{\\&q;_meta\\&q;:{\\&q;id\\&q;:\\&q;'
    parts = html.split(marker)

    seen_ids = set()
    records = []
    raw_records = []

    for part in parts[1:]:
        if '\\&q;address\\&q;:\\&q;' not in part:
            continue

        meta_id = part.split('\\&q;')[0]
        if meta_id in seen_ids:
            continue
        seen_ids.add(meta_id)

        end = part.find('{\\&q;_jsonit\\&q;:{\\&q;_meta\\&q;:{\\&q;id\\&q;:\\&q;', 10)
        if end == -1:
            end = min(len(part), 8000)
        chunk = part[:end]

        def get_str(field):
            m = re.search(r'\\&q;' + field + r'\\&q;:\\&q;([^\\]*)\\&q;', chunk)
            return m.group(1) if m else ''

        def get_num(field):
            m = re.search(r'\\&q;' + field + r'\\&q;:([\d.]+)', chunk)
            return m.group(1) if m else ''

        def get_bool(field):
            m = re.search(r'\\&q;' + field + r'\\&q;:(true|false)', chunk)
            return m.group(1) == 'true' if m else False

        coords_match = re.search(
            r'\\&q;coordinates\\&q;:\[(-?[\d.]+),(-?[\d.]+)\]', chunk
        )
        lng = coords_match.group(1) if coords_match else ''
        lat = coords_match.group(2) if coords_match else ''
        short_name = get_str('shortName')

        record = {
            'property_id': meta_id,
            'address': get_str('address'),
            'city': get_str('city'),
            'state': get_str('state'),
            'zip': get_str('zip'),
            'bedrooms': get_num('bedrooms'),
            'bathrooms': get_num('bathrooms'),
            'sqft': get_num('area'),
            'rent': get_num('rent'),
            'available_at': get_str('availableAt'),
            'market': get_str('market'),
            'unit_status': get_num('unitStatus'),
            'self_tour_status': get_num('selfTourStatus'),
            'special_offer': get_bool('specialOffer'),
            'lat': lat,
            'lng': lng,
            'short_name': short_name,
            'link': f'https://www.firstkeyhomes.com/homes-for-rent/{meta_id}/{short_name}',
            'metro_location': metro_loc,
        }
        records.append(record)
        raw_records.append(record.copy())

    # Check total count from embedded metadata
    decoded = html.replace('\\&q;', '"')
    count_match = re.search(r'"count":(\d+)', decoded)
    total_count = int(count_match.group(1)) if count_match else '?'

    save_json(raw_records, f'firstkey_{metro_loc}.json')
    df = pd.DataFrame(records)
    print(f'  Extracted {len(df)} unique properties (total in market: {total_count})')
    if isinstance(total_count, int) and len(df) < total_count:
        print(f'  Note: {total_count - len(df)} additional listings load via WebSocket')
    return df


# ── Normalize Functions ──────────────────────────────────────────────────────

MASTER_COLS = [
    'source', 'street', 'city', 'state', 'zip', 'beds', 'baths',
    'sqft', 'price', 'status', 'date_available', 'special', 'is_active',
    'lat', 'lng', 'link', 'metro_location', 'first_seen', 'last_seen',
]


def normalize_tricon(df):
    return pd.DataFrame({
        'source': 'Tricon Residential',
        'street': df['address'],
        'city': df['city'],
        'state': df['state'],
        'zip': df['zip_code'],
        'beds': df['bed'],
        'baths': df['bath'],
        'sqft': df['square_feet'],
        'price': df['price'],
        'status': df['available'],
        'date_available': '',
        'special': df['special'],
        'is_active': df['is_active'],
        'lat': None,
        'lng': None,
        'link': df['link'],
        'metro_location': df['metro_location'],
        'first_seen': df['first_seen'],
        'last_seen': df['last_seen'],
    })


def normalize_progress(df):
    return pd.DataFrame({
        'source': 'Progress Residential',
        'street': df['street'],
        'city': df['city'],
        'state': df['state'],
        'zip': df['zip'],
        'beds': df['beds'],
        'baths': df['baths'],
        'sqft': df['sqft'],
        'price': df['current_price'],
        'status': df['property_status'],
        'date_available': df['date_available'],
        'special': df['banner_status'],
        'is_active': df['is_active'],
        'lat': df['lat'],
        'lng': df['lng'],
        'link': df['link'],
        'metro_location': df['metro_location'],
        'first_seen': df['first_seen'],
        'last_seen': df['last_seen'],
    })


def normalize_invh(df):
    return pd.DataFrame({
        'source': 'Invitation Homes',
        'street': df['street'],
        'city': df['city'],
        'state': df['state'],
        'zip': df['zip'],
        'beds': df['beds'],
        'baths': df['baths'],
        'sqft': df['sqft'],
        'price': df['rent'],
        'status': df['status'],
        'date_available': df['available_on'],
        'special': df['is_on_special'].map({True: 'Special Available', False: ''}),
        'is_active': df['is_active'],
        'lat': df['lat'],
        'lng': df['lng'],
        'link': df['link'],
        'metro_location': df['metro_location'],
        'first_seen': df['first_seen'],
        'last_seen': df['last_seen'],
    })


def normalize_amh(df):
    return pd.DataFrame({
        'source': 'AMH',
        'street': df['street'],
        'city': df['city'],
        'state': df['state'],
        'zip': df['zip'],
        'beds': df['beds'],
        'baths': df['baths'],
        'sqft': df['sqft'],
        'price': df['rent'],
        'status': '',
        'date_available': df['available_date'],
        'special': '',
        'is_active': df['is_active'],
        'lat': df['lat'],
        'lng': df['lng'],
        'link': df['link'],
        'metro_location': df['metro_location'],
        'first_seen': df['first_seen'],
        'last_seen': df['last_seen'],
    })


def normalize_msr(df):
    return pd.DataFrame({
        'source': 'Main Street Renewal',
        'street': df['street'],
        'city': df['city'],
        'state': df['state'],
        'zip': df['zip'],
        'beds': df['beds'],
        'baths': df['baths'],
        'sqft': df['sqft'],
        'price': df['rent'],
        'status': df['listing_status'],
        'date_available': df['available_date'],
        'special': df['special'],
        'is_active': df['is_active'],
        'lat': df['lat'],
        'lng': df['lng'],
        'link': df['link'],
        'metro_location': df['metro_location'],
        'first_seen': df['first_seen'],
        'last_seen': df['last_seen'],
    })


def normalize_firstkey(df):
    return pd.DataFrame({
        'source': 'FirstKey Homes',
        'street': df['address'],
        'city': df['city'],
        'state': df['state'],
        'zip': df['zip'],
        'beds': df['bedrooms'],
        'baths': df['bathrooms'],
        'sqft': df['sqft'],
        'price': df['rent'],
        'status': df['special_offer'].map({True: 'Special Offer', False: ''}),
        'date_available': df['available_at'],
        'special': df['special_offer'].map({True: 'Special Offer', False: ''}),
        'is_active': df['is_active'],
        'lat': df['lat'],
        'lng': df['lng'],
        'link': df['link'],
        'metro_location': df['metro_location'],
        'first_seen': df['first_seen'],
        'last_seen': df['last_seen'],
    })


NORMALIZERS = {
    'tricon_listings': normalize_tricon,
    'progress_listings': normalize_progress,
    'invh_listings': normalize_invh,
    'amh_listings': normalize_amh,
    'msr_listings': normalize_msr,
    'firstkey_listings': normalize_firstkey,
}


def build_master_listings():
    """Read all source tables from DB, normalize, and build master_listings."""
    print('\nBuilding master_listings...')
    engine = get_engine()
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()

    frames = []
    for table_name, norm_func in NORMALIZERS.items():
        if table_name in existing_tables:
            df = pd.read_sql(f'SELECT * FROM "{table_name}"', engine)
            normalized = norm_func(df)
            frames.append(normalized)
            active = df['is_active'].sum() if 'is_active' in df.columns else len(df)
            print(f'  {table_name}: {len(df)} total ({active} active)')

    if not frames:
        print('  No source tables found. Run scrapers first.')
        engine.dispose()
        return

    master = pd.concat(frames, ignore_index=True)
    master['price'] = pd.to_numeric(master['price'], errors='coerce')
    master['beds'] = pd.to_numeric(master['beds'], errors='coerce')
    master['baths'] = pd.to_numeric(master['baths'], errors='coerce')
    master['sqft'] = pd.to_numeric(master['sqft'], errors='coerce')

    master.to_sql('master_listings', engine, if_exists='replace', index=False)
    engine.dispose()

    print(f'\n  master_listings: {len(master)} total rows')
    print(master['source'].value_counts().to_string())

    # Summary stats (active only)
    active = master[master['is_active'] == True]
    if len(active) > 0:
        print(f'\n  Active listings: {len(active)}')
        summary = active.groupby('source').agg(
            count=('price', 'size'),
            avg_price=('price', 'mean'),
            min_price=('price', 'min'),
            max_price=('price', 'max'),
            avg_sqft=('sqft', 'mean'),
            avg_beds=('beds', 'mean'),
        ).round(0)
        print(summary.to_string())


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print('=' * 60)
    print('  SFR Listings Scraper')
    print('=' * 60)

    # Metro location input
    metro_city = input('\nEnter metro city (e.g. houston, dallas, atlanta): ').strip().lower()
    metro_state = input('Enter state code (e.g. TX, GA, AZ, FL): ').strip().upper()

    state_full = STATE_FULL_NAMES.get(metro_state)
    if not state_full:
        print(f'Warning: Unknown state code "{metro_state}". Invitation Homes may fail.')
        state_full = metro_state.lower()

    # Build format dict
    metro = {
        'tricon': metro_city,
        'progress': f'{metro_city}-{metro_state.lower()}',
        'invh': f'{metro_city}-{state_full}',
        'amh': metro_city,
        'msr_market': metro_city.title(),
        'msr_state': metro_state,
        'msr': f'{metro_city}-{metro_state.lower()}',
        'firstkey': metro_city,
    }

    # Validate Invitation Homes market
    if metro['invh'] not in INVH_MARKETS:
        print(f'Warning: "{metro["invh"]}" not in Invitation Homes markets list')

    # Validate FirstKey market
    if metro['firstkey'] not in FKH_MARKETS:
        print(f'Warning: "{metro["firstkey"]}" not in FirstKey Homes markets list')

    # Scraper selection
    print('\nWhich scraper(s) do you want to run?')
    print('-' * 40)
    for k, (name, _) in SCRAPERS.items():
        print(f'  {k}. {name}')
    print(f'  7. Run ALL scrapers')
    print()

    choice = input('Enter choice (e.g. 1, 3,5 or 7 for all): ').strip()

    if choice == '7':
        selected = list(SCRAPERS.keys())
    else:
        selected = [c.strip() for c in choice.split(',') if c.strip() in SCRAPERS]

    if not selected:
        print('No valid scrapers selected. Exiting.')
        return

    selected_names = [SCRAPERS[s][0] for s in selected]
    selected_keys = [SCRAPERS[s][1] for s in selected]
    print(f'\nRunning: {", ".join(selected_names)}')
    print('=' * 60)

    # Run scrapers
    for key in selected_keys:
        try:
            if key == 'tricon':
                df = scrape_tricon(metro['tricon'])
            elif key == 'progress':
                df = scrape_progress(metro['progress'])
            elif key == 'invh':
                lat, lng = INVH_MARKETS.get(metro['invh'], (0, 0))
                if lat == 0:
                    print(f'  Skipping Invitation Homes: market "{metro["invh"]}" not found')
                    continue
                df = scrape_invh(metro['invh'], lat, lng)
            elif key == 'amh':
                df = scrape_amh(metro['amh'])
            elif key == 'msr':
                df = scrape_msr(metro['msr_market'], metro['msr_state'], metro['msr'])
            elif key == 'firstkey':
                df = scrape_firstkey(metro['firstkey'])
            else:
                continue

            upsert_to_db(df, TABLE_NAMES[key], UNIQUE_KEYS[key])
            print()

        except Exception as e:
            print(f'  ERROR: {key} failed -- {e}')
            traceback.print_exc()
            print()

    # Build master table
    build_master_listings()

    print('\n' + '=' * 60)
    print('  Done!')
    print(f'  Database: Render Postgres')
    print(f'  JSON files: {JSON_DIR}/')
    print('=' * 60)


if __name__ == '__main__':
    main()
