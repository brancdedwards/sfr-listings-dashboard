"""
SFR Listings Dashboard
Streamlit app for browsing and analyzing single-family rental listings.
"""

import os

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

# ── Config ───────────────────────────────────────────────────────────────────

DATABASE_URL = os.environ.get('DATABASE_URL', '')
if DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

st.set_page_config(
    page_title='SFR Listings Dashboard',
    page_icon='🏠',
    layout='wide',
)


# ── Data Loading ─────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_data():
    """Load master_listings from Postgres, cached for 5 minutes."""
    if not DATABASE_URL:
        return pd.DataFrame()
    engine = create_engine(DATABASE_URL)
    try:
        df = pd.read_sql('SELECT * FROM "master_listings"', engine)
    except Exception:
        return pd.DataFrame()
    finally:
        engine.dispose()

    # Clean types
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    df['beds'] = pd.to_numeric(df['beds'], errors='coerce')
    df['baths'] = pd.to_numeric(df['baths'], errors='coerce')
    df['sqft'] = pd.to_numeric(df['sqft'], errors='coerce')
    df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
    df['lng'] = pd.to_numeric(df['lng'], errors='coerce')
    df['is_active'] = df['is_active'].astype(bool)
    df['date_available'] = pd.to_datetime(df['date_available'], errors='coerce').dt.date
    return df


df = load_data()

if df.empty:
    st.error('No data found. Run the scraper first or check DATABASE_URL.')
    st.stop()


# ── Sidebar Filters ──────────────────────────────────────────────────────────

st.sidebar.header('Filters')

# Active only toggle
active_only = st.sidebar.checkbox('Active listings only', value=True)

# Source filter
sources = sorted(df['source'].unique())
selected_sources = st.sidebar.multiselect('Source', sources, default=sources)

# City filter
cities = sorted(df['city'].dropna().unique())
selected_cities = st.sidebar.multiselect('City', cities)

# Beds filter
bed_min, bed_max = int(df['beds'].min()), int(df['beds'].max())
if bed_min < bed_max:
    beds_range = st.sidebar.slider('Bedrooms', bed_min, bed_max, (bed_min, bed_max))
else:
    beds_range = (bed_min, bed_max)

# Baths filter
bath_min = float(df['baths'].min())
bath_max = float(df['baths'].max())
if bath_min < bath_max:
    baths_range = st.sidebar.slider('Bathrooms', bath_min, bath_max, (bath_min, bath_max), step=0.5)
else:
    baths_range = (bath_min, bath_max)

# Price filter
price_min = int(df['price'].min()) if df['price'].notna().any() else 0
price_max = int(df['price'].max()) if df['price'].notna().any() else 5000
if price_min < price_max:
    price_range = st.sidebar.slider(
        'Monthly Rent ($)',
        price_min, price_max, (price_min, price_max),
        step=50,
    )
else:
    price_range = (price_min, price_max)

# Sqft filter
sqft_min = int(df['sqft'].min()) if df['sqft'].notna().any() else 0
sqft_max = int(df['sqft'].max()) if df['sqft'].notna().any() else 5000
if sqft_min < sqft_max:
    sqft_range = st.sidebar.slider(
        'Square Feet',
        sqft_min, sqft_max, (sqft_min, sqft_max),
        step=100,
    )
else:
    sqft_range = (sqft_min, sqft_max)

# Apply filters
filtered = df.copy()
if active_only:
    filtered = filtered[filtered['is_active'] == True]
if selected_sources:
    filtered = filtered[filtered['source'].isin(selected_sources)]
if selected_cities:
    filtered = filtered[filtered['city'].isin(selected_cities)]
filtered = filtered[
    (filtered['beds'] >= beds_range[0]) & (filtered['beds'] <= beds_range[1])
]
filtered = filtered[
    (filtered['baths'] >= baths_range[0]) & (filtered['baths'] <= baths_range[1])
]
filtered = filtered[
    (filtered['price'] >= price_range[0]) & (filtered['price'] <= price_range[1])
]
filtered = filtered[
    (filtered['sqft'] >= sqft_range[0]) & (filtered['sqft'] <= sqft_range[1])
]

# ── Sort options ─────────────────────────────────────────────────────────────

SORTABLE_COLS = {
    'Price': 'price',
    'Sqft': 'sqft',
    'Beds': 'beds',
    'Baths': 'baths',
    'City': 'city',
    'Source': 'source',
    'Available': 'date_available',
}

st.sidebar.markdown('---')
st.sidebar.header('Sort')

sort_col_1 = st.sidebar.selectbox('Sort by', list(SORTABLE_COLS.keys()), index=0)
sort_dir_1 = st.sidebar.radio('Direction', ['Ascending', 'Descending'], index=1, key='dir1', horizontal=True)

sort_col_2 = st.sidebar.selectbox('Then by', ['None'] + list(SORTABLE_COLS.keys()), index=0)
sort_dir_2 = st.sidebar.radio('Direction', ['Ascending', 'Descending'], index=1, key='dir2', horizontal=True) if sort_col_2 != 'None' else None

sort_cols = [SORTABLE_COLS[sort_col_1]]
sort_asc = [sort_dir_1 == 'Ascending']

if sort_col_2 != 'None':
    sort_cols.append(SORTABLE_COLS[sort_col_2])
    sort_asc.append(sort_dir_2 == 'Ascending')

filtered = filtered.sort_values(sort_cols, ascending=sort_asc, na_position='last')


# ── Header ───────────────────────────────────────────────────────────────────

st.title('SFR Listings Dashboard')

col1, col2, col3, col4 = st.columns(4)
col1.metric('Total Listings', f'{len(filtered):,}')
col2.metric('Avg Rent', f'${filtered["price"].mean():,.0f}' if len(filtered) else '$0')
col3.metric('Avg Sqft', f'{filtered["sqft"].mean():,.0f}' if len(filtered) else '0')
col4.metric('Sources', f'{filtered["source"].nunique()}')


# ── Tabs ─────────────────────────────────────────────────────────────────────

tab_listings, tab_analytics = st.tabs(['Listings', 'Analytics'])


# ── Helper: CSV converter ────────────────────────────────────────────────────

@st.cache_data
def to_csv(dataframe):
    return dataframe.to_csv(index=False).encode('utf-8')


# ── Tab 1: Listings ──────────────────────────────────────────────────────────

with tab_listings:
    display_cols = [
        'source', 'street', 'city', 'state', 'zip', 'beds', 'baths',
        'sqft', 'price', 'status', 'date_available', 'special', 'link',
    ]
    available_cols = [c for c in display_cols if c in filtered.columns]

    st.dataframe(
        filtered[available_cols],
        use_container_width=True,
        height=600,
        column_config={
            'link': st.column_config.LinkColumn('Link', display_text='View'),
            'price': st.column_config.NumberColumn('Rent', format='$%d'),
            'sqft': st.column_config.NumberColumn('Sqft', format='%d'),
            'beds': st.column_config.NumberColumn('Beds', format='%d'),
            'baths': st.column_config.NumberColumn('Baths', format='%.1f'),
            'date_available': st.column_config.DateColumn('Available', format='MM/DD/YYYY'),
        },
    )
    st.caption(f'Showing {len(filtered)} of {len(df)} total listings')

    # Download buttons
    dl_col1, dl_col2, _ = st.columns([1, 1, 4])
    with dl_col1:
        st.download_button(
            f'Download filtered ({len(filtered)})',
            to_csv(filtered[available_cols]),
            'sfr_listings_filtered.csv',
            'text/csv',
        )
    with dl_col2:
        st.download_button(
            f'Download all ({len(df)})',
            to_csv(df[available_cols]),
            'sfr_listings_all.csv',
            'text/csv',
        )


# ── Tab 2: Analytics ─────────────────────────────────────────────────────────

with tab_analytics:
    if len(filtered) == 0:
        st.warning('No listings match the current filters.')
    else:
        # Row 1: Listings count + Avg price by source
        col_a, col_b = st.columns(2)

        with col_a:
            count_by_source = filtered.groupby('source').size().reset_index(name='count')
            fig = px.bar(
                count_by_source, x='source', y='count',
                title='Listings by Source',
                color='source',
                text='count',
            )
            fig.update_layout(showlegend=False, xaxis_title='', yaxis_title='Count')
            st.plotly_chart(fig, use_container_width=True)

        with col_b:
            avg_price = filtered.groupby('source')['price'].mean().reset_index()
            avg_price.columns = ['source', 'avg_price']
            fig = px.bar(
                avg_price, x='source', y='avg_price',
                title='Average Rent by Source',
                color='source',
                text=avg_price['avg_price'].apply(lambda x: f'${x:,.0f}'),
            )
            fig.update_layout(showlegend=False, xaxis_title='', yaxis_title='Avg Rent ($)')
            st.plotly_chart(fig, use_container_width=True)

        # Row 2: Price distribution + Price vs Sqft
        col_c, col_d = st.columns(2)

        with col_c:
            fig = px.histogram(
                filtered, x='price', color='source',
                title='Price Distribution',
                nbins=30,
                barmode='overlay',
                opacity=0.7,
            )
            fig.update_layout(xaxis_title='Monthly Rent ($)', yaxis_title='Count')
            st.plotly_chart(fig, use_container_width=True)

        with col_d:
            scatter_df = filtered.dropna(subset=['sqft', 'price'])
            if len(scatter_df) > 0:
                fig = px.scatter(
                    scatter_df, x='sqft', y='price', color='source',
                    title='Price vs Square Footage',
                    hover_data=['street', 'city', 'beds'],
                    opacity=0.6,
                )
                fig.update_layout(xaxis_title='Sqft', yaxis_title='Rent ($)')
                st.plotly_chart(fig, use_container_width=True)

        # Row 3: Beds distribution + Price by beds
        col_e, col_f = st.columns(2)

        with col_e:
            beds_count = filtered['beds'].value_counts().sort_index().reset_index()
            beds_count.columns = ['beds', 'count']
            fig = px.bar(
                beds_count, x='beds', y='count',
                title='Listings by Bedroom Count',
                text='count',
            )
            fig.update_layout(xaxis_title='Bedrooms', yaxis_title='Count')
            st.plotly_chart(fig, use_container_width=True)

        with col_f:
            price_by_beds = filtered.groupby('beds')['price'].mean().reset_index()
            price_by_beds.columns = ['beds', 'avg_price']
            fig = px.bar(
                price_by_beds, x='beds', y='avg_price',
                title='Average Rent by Bedroom Count',
                text=price_by_beds['avg_price'].apply(lambda x: f'${x:,.0f}'),
            )
            fig.update_layout(xaxis_title='Bedrooms', yaxis_title='Avg Rent ($)')
            st.plotly_chart(fig, use_container_width=True)
