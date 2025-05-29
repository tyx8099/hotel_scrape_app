"""
Hotel Room Price Scraper - A Streamlit web application for scraping hotel room prices from Booking.com.

This application allows users to:
1. Select a country and currency
2. Input multiple hotel URL names
3. Choose a start date for price checking
4. Fetch and display room prices and details
5. Download results as CSV

The app uses async HTTP requests to efficiently fetch data from multiple hotels and date ranges.
"""

# Standard library imports
import asyncio
import random
import re
import time
from datetime import datetime, timedelta
from typing import List, Tuple

# Third-party imports
import aiohttp
import pandas as pd
import pycountry
import streamlit as st
from bs4 import BeautifulSoup

# Constants
DEFAULT_MAX_CONCURRENT = 10
DEFAULT_SEARCH_DAYS = 365
DEFAULT_STAY_DURATION = 7
DEFAULT_CURRENCY = "USD"
USER_AGENT = 'Mozilla/5.0 (X11; CrOS x86_64 8172.45.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.64 Safari/537.36'

# Web scraping functions
async def fetch_hotel_page(session: aiohttp.ClientSession, url: str) -> str:
    """Fetch HTML content of a hotel page using aiohttp."""
    headers = {
        'User-Agent': USER_AGENT,
        'Accept-Language': 'en-US, en;q=0.5'
    }
    try:
        async with session.get(url, headers=headers) as response:
            response.raise_for_status()
            return await response.text()
    except Exception as e:
        print(f"Error fetching {url}: {str(e)}")
        return ""

def extract_room_area(row) -> Tuple[float, str]:
    """Extract room area information from HTML row."""
    try:
        area_elements = row.find_all(['span', 'div'], class_=lambda x: x and any(
            cls in str(x) for cls in ['bui-badge', 'room-size', 'facility', 'hprt-facility']
        ))
        
        for element in area_elements:
            text = ' '.join(element.stripped_strings)
            match = re.search(
                r'(\d+[.,]?\d*)\s*(?:square\s*)?(feet²|ft²|sq\s*ft|m²|sqm|sq\s*m|meters²)',
                text,
                re.IGNORECASE
            )
            
            if match:
                area_value = match.group(1).replace(',', '')
                area_unit = match.group(2).lower()
                room_area = float(area_value) if '.' in area_value else int(area_value)
                return (room_area, area_unit)
                
        return None
    except Exception as e:
        print(f"Error extracting room area: {e}")
        return None

def extract_room_price(row) -> str:
    """Extract room price from HTML row."""
    try:
        # Check common price display element
        price_element = row.find('span', class_='prco-valign-middle-helper')
        if price_element:
            price_text = price_element.get_text(strip=True)
            match = re.search(r'[\d,]+\.?\d*', price_text)
            if match:
                return match.group().replace(',', '')

        # Check screen-reader text element
        sr_only_elements = row.find_all('span', class_='bui-u-sr-only')
        for element in sr_only_elements:
            price_text = element.get_text(strip=True)
            if "price" in price_text.lower():
                match = re.findall(r'[\d,]+\.?\d*', price_text)
                if match:
                    return match[-1].replace(',', '')

        return None
    except Exception as e:
        print(f"Error extracting room price: {e}")
        return None

async def parse_hotel_page(html: str, hotel_name: str, check_in_date: str, 
                         check_out_date: str, url: str) -> pd.DataFrame:
    """Parse hotel page HTML and extract room details."""
    soup = BeautifulSoup(html, 'html.parser')
    data = []
    
    display_name = soup.find('h2', {'class': 'hp__hotel-name'})
    hotel_display_name = display_name.get_text(strip=True) if display_name else hotel_name
    
    tables = soup.find_all('table', class_='hprt-table')
    for table in tables:
        for row in table.find_all('tr', {'data-block-id': True}):
            room_name = row.find('span', class_='hprt-roomtype-icon-link')
            room_name = room_name.get_text(strip=True) if room_name else None
            
            room_price = extract_room_price(row)
            area_info = extract_room_area(row)
            
            if area_info:
                room_area, area_unit = area_info
                data.append({
                    'hotel_name': hotel_display_name,
                    'check_in_date': check_in_date,
                    'check_out_date': check_out_date,
                    'room_name': room_name,
                    'room_price': room_price,
                    'room_area': room_area,
                    'area_unit': area_unit,
                    'url': url
                })
    
    return pd.DataFrame(data)

# Data gathering functions
async def get_hotel_details_async(session: aiohttp.ClientSession, hotel_url_name: str, 
                                check_in_date: str, check_out_date: str, 
                                country: str, currency: str) -> pd.DataFrame:
    """Fetch and parse hotel details for a specific date range."""
    url = f'https://www.booking.com/hotel/{country}/{hotel_url_name}.en-gb.html?checkin={check_in_date};checkout={check_out_date};dist=0;group_adults=2;group_children=0;selected_currency={currency}'
    
    html = await fetch_hotel_page(session, url)
    if not html:
        return pd.DataFrame()
    
    hotel_details = await parse_hotel_page(html, hotel_url_name, check_in_date, check_out_date, url)
    return hotel_details

async def gather_hotel_data(hotel_list: List[str], date_ranges: List[Tuple[str, str]], 
                          country: str, currency: str, progress_callback=None, 
                          max_concurrent: int = DEFAULT_MAX_CONCURRENT) -> pd.DataFrame:
    """Gather hotel data for all hotels and date ranges asynchronously."""
    connector = aiohttp.TCPConnector(limit=max_concurrent)
    timeout = aiohttp.ClientTimeout(total=30)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = []
        total_requests = len(hotel_list) * len(date_ranges)
        completed_requests = 0
        
        for hotel_name in hotel_list:
            for date_range in date_ranges:
                task = get_hotel_details_async(
                    session, hotel_name, date_range[0], date_range[1], 
                    country, currency
                )
                tasks.append(task)
        
        results = []
        for future in asyncio.as_completed(tasks):
            result = await future
            results.append(result)
            completed_requests += 1
            if progress_callback:
                progress_callback(completed_requests, total_requests)
        
        valid_dfs = [df for df in results if isinstance(df, pd.DataFrame) and not df.empty]
        return pd.concat(valid_dfs, ignore_index=True) if valid_dfs else pd.DataFrame()

def main_async(hotel_list: List[str], date_ranges: List[Tuple[str, str]], 
               country: str = "sg", currency: str = "SGD", 
               progress_callback=None) -> pd.DataFrame:
    """Main async function to gather all hotel data."""
    return asyncio.run(gather_hotel_data(
        hotel_list, date_ranges, country, currency, progress_callback
    ))

# Date utility functions
def generate_date_ranges(start_date: datetime.date, delta: int = DEFAULT_SEARCH_DAYS) -> List[Tuple[str, str]]:
    """Generate random date ranges within each month for a given time period."""
    if not start_date:
        return []
        
    end_date = start_date + timedelta(days=delta)
    date_ranges = []
    current_date = start_date
    
    while current_date < end_date:
        month_start = current_date.replace(day=1)
        next_month = month_start.replace(day=28) + timedelta(days=4)
        month_end = next_month - timedelta(days=next_month.day)
        
        start_day = random.randint(1, max(1, month_end.day - DEFAULT_STAY_DURATION))
        range_start = month_start.replace(day=start_day)
        range_end = range_start + timedelta(days=DEFAULT_STAY_DURATION)
        
        date_ranges.append((
            range_start.strftime("%Y-%m-%d"), 
            range_end.strftime("%Y-%m-%d")
        ))
        current_date = month_end + timedelta(days=1)
    
    return date_ranges

# UI components
def country_currency_selectors() -> Tuple[str, str]:
    """Create country and currency selection widgets."""
    col1, col2 = st.columns(2)
    
    with col1:
        countries = [(country.name, country.alpha_2) for country in pycountry.countries]
        countries.sort()
        country_code = st.selectbox(
            "Select Country",
            countries,
            format_func=lambda x: x[0],
            index=countries.index(("United States", "US"))
        )[1]
    
    with col2:
        currency = st.selectbox(
            "Select Currency",
            ["USD", "EUR", "SGD"],
            index=0
        )
    
    return country_code, currency

def multi_string_input(label: str = "Enter hotel names (one per line)",
                      default_items: List[str] = None,
                      key: str = "hotel_names") -> List[str]:
    """Create a text area for bulk hotel name input."""
    if default_items is None:
        default_items = [""]
    
    if key not in st.session_state:
        st.session_state[key] = "\n".join(default_items)
    
    st.markdown(f"**{label}**")
    bulk_strings = st.text_area(
        "",
        st.session_state[key],
        key=f"{key}_bulk",
        height=150,
        label_visibility="collapsed"
    )
    st.session_state[key] = bulk_strings
    
    strings = [s.strip() for s in st.session_state[key].split('\n') if s.strip()]
    st.caption(f"Found {len(strings)} hotels")
    
    return list(map(lambda x: x.lower(), strings))

def process_results(df: pd.DataFrame) -> pd.DataFrame:
    """Process and format the results DataFrame."""
    if df.empty:
        return df
        
    df = df.dropna(subset=['room_name'])
    df = (df
          .sort_values('room_price')
          .groupby(['check_in_date', 'check_out_date', 'room_name'], as_index=False)
          .first()
          .sort_values(['check_in_date', 'room_price'])
          .reset_index(drop=True))
    
    column_order = ['hotel_name'] + [col for col in df.columns if col != 'hotel_name']
    return df[column_order]

# Main UI
def main():
    st.set_page_config(
        page_title="Hotel Scraper",
        page_icon="🏨",
        layout="wide"
    )

    st.title("Hotel Room Price Scraper")

    country, currency = country_currency_selectors()
    st.write(f"Selected: {country} | {currency}")

    hotel_list = multi_string_input("Enter hotel URL names (one per line):")
    st.write("Current items:", hotel_list)

    start_date = st.date_input("Select a start date:")

    if st.button("Process Data"):
        if hotel_list and start_date:
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            def update_progress(current, total):
                progress = current / total
                progress_bar.progress(progress)
                status_text.text(f"Processing {current} of {total} requests...")
            
            date_ranges = generate_date_ranges(start_date)
            result_df = main_async(
                hotel_list, 
                date_ranges, 
                country=country.lower(), 
                currency=currency,
                progress_callback=update_progress
            )
            
            progress_bar.empty()
            status_text.empty()
            
            if len(result_df) > 0:
                result_df = process_results(result_df)
                
                st.success("Data processing complete!")
                st.dataframe(result_df)
                
                csv = result_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name='hotel_prices.csv',
                    mime='text/csv'
                )
            else:
                st.warning("No data found. Please check your inputs and try again.")
        else:
            st.warning("Please enter at least one hotel name and select a start date. Hotel must exist within the country selected.")

if __name__ == "__main__":
    main()
