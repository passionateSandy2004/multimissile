# Universal Product Extractor for E-Commerce Result Pages
"""
Universal extractor that parses product listings from arbitrary e-commerce
search/result pages using layered, comprehensive selector strategies and
robust fallbacks (including schema.org JSON-LD parsing).

Design mirrors `LaunchPad/universalSearch.py` for consistency:
- Extensive CSS/XPath selector families
- Selenium-based DOM discovery with smart waits
- Normalization utilities (price parsing, URL resolution, text cleanup)
- Optional JSON-LD/schema.org extraction as a fallback
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException
try:
    from webdriver_manager.chrome import ChromeDriverManager
    WEBDRIVER_MANAGER_AVAILABLE = True
except ImportError:
    WEBDRIVER_MANAGER_AVAILABLE = False
from urllib.parse import urlparse, urljoin
import json
import re
import time
import os
import threading
import uuid
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Iterable, Union, Tuple

# Supabase imports
try:
    from supabase import create_client, Client
    SUPABASE_AVAILABLE = True
except ImportError:
    SUPABASE_AVAILABLE = False
    print("[!] Warning: supabase-py not installed. Install with: pip install supabase")

# Supabase configuration
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://whfjofihihlhctizchmj.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndoZmpvZmloaWhsaGN0aXpjaG1qIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjEzNzQzNDMsImV4cCI6MjA3Njk1MDM0M30.OsJnOqeJgT5REPg7uxkGmmVcHIcs5QO4vdyDi66qpR0")

_SUPABASE_CLIENT_LOCK = threading.Lock()
_SUPABASE_CLIENT: Optional[Client] = None


def _get_supabase_client() -> Optional[Client]:
    global _SUPABASE_CLIENT
    if not SUPABASE_AVAILABLE or not SUPABASE_KEY:
        return None
    with _SUPABASE_CLIENT_LOCK:
        if _SUPABASE_CLIENT is None:
            try:
                _SUPABASE_CLIENT = create_client(SUPABASE_URL, SUPABASE_KEY)
            except Exception as exc:
                print(f"[!] Failed to initialize Supabase client: {exc}")
                _SUPABASE_CLIENT = None
        return _SUPABASE_CLIENT


class UniversalProductExtractor:
    """
    Extract product data (title, price, image, link, availability, ratings, etc.)
    from any e-commerce listing/search page using layered strategies.
    """

    def __init__(self):
        self.selector_sets = self._build_selector_sets()
        self._thread_local = threading.local()
        self._drivers_lock = threading.Lock()
        self._active_drivers = set()
        
        # Initialize Supabase connection
        self.supabase: Optional[Client] = None
        if SUPABASE_AVAILABLE and SUPABASE_KEY:
            self.supabase = _get_supabase_client()
            if self.supabase:
                print("[✓] Connected to Supabase for product storage\n")
            else:
                print("[!] Warning: Failed to connect to Supabase. Products will not be saved to database\n")
        elif not SUPABASE_AVAILABLE:
            print("[!] Warning: supabase-py not installed. Products will not be saved to database\n")
        elif not SUPABASE_KEY:
            print("[!] Warning: SUPABASE_KEY not set. Products will not be saved to database\n")

        # Heuristic phrases and keywords used across strategies
        self.no_results_phrases = [
            'no results',
            'no results found',
            'no result found',
            '0 results',
            '0 result',
            'no product',
            'nothing found',
            'did not find anything',
            'did not find anythings',
            'we did not find',
            'we did not find anything',
            'we did not find anythings',
            'try another search',
            'try a different search',
        ]

        self.link_blacklist_keywords = [
            'login', 'register', 'signup', 'account', 'profile', 'help', 'faq', 'contact',
            'privacy', 'terms', 'policy', 'cart', 'wishlist', 'checkout', 'track', 'order',
            'facebook', 'instagram', 'whatsapp', 'twitter', 'youtube', 'pinterest',
            'linkedin', 'support', 'mailto:', 'tel:', 'javascript:', 'gift-card', 'loyalty',
        ]

        self.product_path_keywords = [
            '/product', '/products', '/item', '/items', '/p/', '/dp/', '/pd/', '/pdp',
            '/shop/', '/store/', '/catalog', '/listing', '/sku', '/detail', '/details',
            '/gp/', '/gp/product', '/listing/', '/prod', '/itm', '/itm/',
            'collection', 'collections', 'category', 'categories',
            'productId', 'sku=', 'pid=', 'variant=', 'model=', '/buy/', '/sale/',
        ]

        self.blacklisted_sections = {'header', 'nav', 'footer', 'aside', 'form'}

        self.load_more_selectors = [
            'button[class*="load" i]',
            'button[id*="load" i]',
            'button[data-test*="load" i]',
            'button[data-testid*="load" i]',
            'button[aria-label*="load" i]',
            'button[class*="more" i]',
            'a[class*="load" i]',
            'div[class*="load-more" i]',
            '[data-action*="loadMore" i]',
        ]

        self.popup_close_selectors = [
            'button[aria-label*="close" i]',
            'button[class*="close" i]',
            'button[class*="dismiss" i]',
            '[role="dialog"] button',
            '.close-button',
            '.modal-close',
            '.overlay-close',
            '[data-testid*="close" i]',
            '[data-action*="close" i]',
            '[aria-label*="dismiss" i]',
        ]

        self.max_scroll_attempts = 4

    # ------------------------------------------------------------------
    # Driver lifecycle helpers
    # ------------------------------------------------------------------

    def _get_or_create_driver(self) -> webdriver.Chrome:
        driver = getattr(self._thread_local, "driver", None)
        if driver is None:
            driver = self._setup_driver()
            self._thread_local.driver = driver
            with self._drivers_lock:
                self._active_drivers.add(driver)
        return driver

    def _reset_thread_driver(self):
        driver = getattr(self._thread_local, "driver", None)
        if not driver:
            return
        try:
            driver.quit()
        except Exception:
            pass
        with self._drivers_lock:
            self._active_drivers.discard(driver)
        try:
            del self._thread_local.driver
        except AttributeError:
            pass

    def close_reusable_driver(self):
        """Close the reusable driver bound to the current worker thread."""
        self._reset_thread_driver()

    def _close_all_drivers(self):
        with self._drivers_lock:
            drivers = list(self._active_drivers)
            self._active_drivers.clear()
        for driver in drivers:
            try:
                driver.quit()
            except Exception:
                pass

    def shutdown(self):
        """Close any drivers kept alive for reuse."""
        self._close_all_drivers()

    def __del__(self):
        try:
            self.shutdown()
        except Exception:
            pass

    def _build_selector_sets(self) -> Dict[str, List[str]]:
        """Define comprehensive selectors for product cards and fields."""
        return {
            # Common result container scopes (helps avoid grabbing banners/footers)
            "result_containers": [
                'ul.products',
                'ul.product-list',
                'ul.search-results',
                'div.products',
                'div.product-list',
                'div.search-results',
                'div[class*="listing" i]',
                'div[class*="product-grid" i]',
                'div[data-component*="product" i]',
                'div[data-testid*="result" i]',
                'section[class*="grid" i]',
                'section[class*="listing" i]',
                'section[class*="catalog" i]',
                'div[class*="grid" i]',
                'section[class*="product" i]',
                'section[class*="result" i]',
                'main',
            ],
            # Product card/container candidates
            "product_cards": [
                '[data-component="product"]',
                '[data-qa*="product" i]',
                '[data-testid*="product" i]',
                '[data-cy*="product" i]',
                '[itemscope][itemtype*="schema.org/Product" i]',
                'div[data-product-id]',
                'article[data-product-id]',
                'div[data-asin]',
                'li[data-asin]',
                'li[data-id*="product" i]',
                'div[data-testid*="product-card" i]',
                'li[class*="product" i]',
                'li[class*="grid" i]',
                'div[class*="product" i]',
                'div[class*="item" i]',
                'div[class*="card" i]',
                'div[class*="result" i]',
                'article[class*="product" i]',
                'article[class*="item" i]',
            ],

            # Title within a card
            "title": [
                '[itemprop="name"]',
                'a[title]',
                'a[class*="title" i]',
                'a[data-testid*="title" i]',
                'h1', 'h2', 'h3', 'h4',
                '[class*="title" i]',
                '[class*="name" i]',
                '[aria-label*="product" i]',
            ],

            # Link within a card
            "link": [
                'a[href*="/product" i]',
                'a[href*="/item" i]',
                'a[href*="/p/" i]',
                'a[href*="?pid=" i]',
                'a[data-testid*="product" i]',
                'a[data-track*="product" i]',
                'a[href]',
                '[itemprop="url"]',
            ],

            # Image within a card
            "image": [
                'img[src]',
                'img[data-src]',
                'img[data-original]',
                'img[data-lazy-src]',
                'img[data-srcset]',
                'source[data-srcset]',
                '[data-background-image]',
                '[itemprop="image"]',
            ],

            # Price within a card
            "price": [
                '[itemprop="price"]',
                '[class*="price" i]',
                '[class*="offer" i]',
                '[data-price]',
                'span[data-price]',
                'div[data-price]',
                'span[class*="amount" i]',
                'span[class*="value" i]',
                'meta[itemprop="price"][content]',
            ],

            # Currency within a card
            "currency": [
                'meta[itemprop="priceCurrency"][content]',
                '[class*="currency" i]',
                'span[data-currency]',
            ],

            # Rating within a card
            "rating": [
                '[itemprop="ratingValue"]',
                '[class*="rating" i]',
                '[aria-label*="rating" i]',
            ],

            # Reviews count
            "reviews": [
                '[itemprop="reviewCount"]',
                '[class*="review" i]',
                '[aria-label*="review" i]',
            ],

            # Availability
            "availability": [
                '[itemprop="availability"]',
                '[class*="stock" i]',
                '[class*="avail" i]',
            ],

            # Brand
            "brand": [
                '[itemprop="brand"]',
                '[class*="brand" i]',
                '[data-brand]',
            ],

            # SKU / product code
            "sku": [
                '[itemprop="sku"]',
                '[data-sku]',
                '[data-product-sku]',
                '[class*="sku" i]',
            ],

            # Description snippet
            "description": [
                '[itemprop="description"]',
                '[class*="description" i]',
                '[class*="subtitle" i]',
                'p',
            ],
        }

    def extract_products(
        self,
        url: str,
        max_items: int = 50,
        wait_seconds: int = 12,
        product_type_id: Optional[int] = None,
        searched_product_id: Optional[int] = None,
        reuse_driver: bool = False,
        url_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Load the page and extract a list of products.

        Args:
            url: URL to extract products from
            max_items: Maximum number of products to extract
            wait_seconds: Wait time for page load
            product_type_id: ID of the product type from product_type_table (optional)
            searched_product_id: ID of the product from products table that was searched for (optional)
            reuse_driver: When True, reuse a thread-bound Selenium driver instead of creating
                and tearing one down per call. Intended for worker pool scenarios.

        Returns a dict containing metadata and an array of product dicts.
        """
        attempt = 0
        max_attempts = 2 if reuse_driver else 1
        last_error: Optional[Exception] = None

        while attempt < max_attempts:
            driver = None
            managed_driver = False
            try:
                if reuse_driver:
                    driver = self._get_or_create_driver()
                    # Clear any residual state from previous URL when reusing driver
                    try:
                        # Clear cookies and cache to prevent state pollution
                        driver.delete_all_cookies()
                    except Exception:
                        pass  # Ignore if cookies can't be cleared
                else:
                    driver = self._setup_driver()
                    managed_driver = True

                print(f"[Universal Extractor] Navigating: {url}")
                driver.get(url)

                # Wait for the DOM to be ready
                WebDriverWait(driver, wait_seconds).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )

                # Handle popups/load-more/infinite scroll before extraction
                self._dismiss_known_popups(driver)
                self._progressive_scroll_and_load(driver)

                # Try to wait for any of the product card selectors after prep
                self._wait_for_any_selector(driver, self.selector_sets["product_cards"], wait_seconds)

                # Strategy 1: DOM-based extraction within scoped containers
                products = self._extract_from_dom(driver, url, max_items)

                # If nothing found, try JSON-LD fallback
                if not products:
                    products = self._extract_from_jsonld(driver, url, max_items)

                # Structured data via microdata (itemscope/itemprop)
                if len(products) == 0:
                    products = self._extract_from_microdata(driver, url, max_items)

                # Inline JSON data structures (application/json scripts)
                if len(products) == 0:
                    products = self._extract_from_inline_data_scripts(driver, url, max_items)

                # Strategy 2: Heuristic global scan if still weak results
                if len(products) == 0:
                    products = self._extract_by_global_heuristics(driver, url, max_items)

                # Strategy 3: Last resort - anchors that look like products (image + product-like path)
                if len(products) == 0:
                    products = self._extract_from_links_with_images(driver, url, max_items)

                # If still nothing and page clearly indicates "no results", return empty
                if not products and self._page_indicates_no_results(driver):
                    return {
                        "success": True,
                        "page_url": url,
                        "platform": urlparse(url).netloc,
                        "num_products": 0,
                        "products": [],
                    }

                # Deduplicate by product_url
                products = self._dedupe_by_url(products)
                if len(products) > max_items:
                    products = products[:max_items]

                # Get platform URL from the extracted URL
                platform_url = url
                platform = urlparse(url).netloc

                # Save products to database (with product type and searched product info)
                saved_count = self._save_products_to_db(
                    products,
                    platform_url,
                    platform,
                    product_type_id=product_type_id,
                    searched_product_id=searched_product_id,
                )

                return {
                    "success": True,
                    "page_url": url,
                    "platform": platform,
                    "num_products": len(products),
                    "products": products,
                    "saved_to_db": saved_count,
                    "url_id": url_id,
                }

            except Exception as e:
                last_error = e
                if reuse_driver:
                    self._reset_thread_driver()
                else:
                    if managed_driver and driver:
                        try:
                            driver.quit()
                        except Exception:
                            pass
                    driver = None

                if not reuse_driver:
                    break

            finally:
                if managed_driver and driver:
                    try:
                        driver.quit()
                    except Exception:
                        pass

            attempt += 1

        return {
            "success": False,
            "page_url": url,
            "error": str(last_error) if last_error else "Unknown error",
            "url_id": url_id,
        }

    # ----------------------------- DOM Extraction -----------------------------

    def _extract_from_dom(self, driver: webdriver.Chrome, base_url: str, max_items: int) -> List[Dict[str, Any]]:
        products: List[Dict[str, Any]] = []

        # Scope search to likely result containers first
        container_elements = self._find_first_nonempty_set(
            driver, self.selector_sets.get("result_containers", []), By.CSS_SELECTOR
        )

        card_elements = []
        if container_elements:
            for cont in container_elements:
                try:
                    for sel in self.selector_sets["product_cards"]:
                        els = cont.find_elements(By.CSS_SELECTOR, sel)
                        card_elements.extend([e for e in els if e.is_displayed()])
                except Exception:
                    continue
        else:
            card_elements = self._find_first_nonempty_set(
                driver, self.selector_sets["product_cards"], By.CSS_SELECTOR
            )

        # If no obvious cards, try a permissive guess: any li/div with link+image
        if not card_elements:
            candidates = driver.find_elements(By.CSS_SELECTOR, "li, div, article")
            card_elements = [el for el in candidates if self._looks_like_product_card(el)]

        accepted = 0
        for card in card_elements:
            try:
                if self._is_within_blacklisted_section(card):
                    continue
                product = self._extract_fields_from_card(card, base_url)
                if product and self._is_valid_product(product, base_url):
                    products.append(product)
                    accepted += 1
                    if accepted >= max_items:
                        break
            except Exception:
                continue

        return products

    def _looks_like_product_card(self, el) -> bool:
        try:
            has_link = False
            has_image = False
            try:
                el.find_element(By.CSS_SELECTOR, "a[href]")
                has_link = True
            except Exception:
                pass
            try:
                el.find_element(By.CSS_SELECTOR, "img[src], img[data-src], img[data-original]")
                has_image = True
            except Exception:
                pass
            text = (el.text or "").lower()
            priceish = any(tok in text for tok in ["$", "₹", "rs.", "rs ", "usd", "eur", "price"])  # quick heuristic
            return has_link and (has_image or priceish)
        except Exception:
            return False

    def _extract_fields_from_card(self, card, base_url: str) -> Dict[str, Any]:
        def find_text(selectors: List[str]) -> Optional[str]:
            for sel in selectors:
                try:
                    el = card.find_element(By.CSS_SELECTOR, sel)
                    txt = el.get_attribute("content") or el.get_attribute("aria-label") or el.text
                    txt = self._clean_text(txt)
                    if txt:
                        return txt
                except Exception:
                    continue
            return None

        def find_attr(selectors: List[str], attr: str) -> Optional[str]:
            for sel in selectors:
                try:
                    el = card.find_element(By.CSS_SELECTOR, sel)
                    val = el.get_attribute(attr)
                    if val:
                        return val
                except Exception:
                    continue
            return None

        # Prefer link text as title if available
        title = None
        try:
            a = card.find_element(By.CSS_SELECTOR, 'a[href]')
            title = self._clean_text(a.get_attribute('title') or a.text)
        except Exception:
            pass
        # Fallback to image alt if still missing
        if not title:
            try:
                img = card.find_element(By.CSS_SELECTOR, 'img')
                title = self._clean_text(img.get_attribute('alt'))
            except Exception:
                pass
        if not title:
            title = find_text(self.selector_sets["title"]) or None

        # Prefer link from the most specific selector order
        link_href = None
        for sel in self.selector_sets["link"]:
            try:
                el = card.find_element(By.CSS_SELECTOR, sel)
                href = el.get_attribute("href") or el.get_attribute("content")
                if href:
                    link_href = href
                    break
            except Exception:
                continue

        image_src = None
        for sel in self.selector_sets["image"]:
            try:
                el = card.find_element(By.CSS_SELECTOR, sel)
                image_src = (
                    el.get_attribute("src")
                    or el.get_attribute("data-src")
                    or el.get_attribute("data-original")
                    or el.get_attribute("data-srcset")
                    or el.get_attribute("content")
                )
                if image_src:
                    break
            except Exception:
                continue

        # Price and currency
        raw_price = None
        for sel in self.selector_sets["price"]:
            try:
                el = card.find_element(By.CSS_SELECTOR, sel)
                raw_price = el.get_attribute("content") or el.text
                raw_price = self._clean_text(raw_price)
                if raw_price:
                    break
            except Exception:
                continue

        currency = None
        for sel in self.selector_sets["currency"]:
            try:
                el = card.find_element(By.CSS_SELECTOR, sel)
                currency = el.get_attribute("content") or el.text
                currency = self._clean_text(currency)
                if currency:
                    break
            except Exception:
                continue

        # Try parsing price from entire card text if selector missed
        if not raw_price:
            try:
                raw_price = self._extract_price_from_text(card.text)
            except Exception:
                pass

        parsed_price, detected_currency = self._parse_price(raw_price)
        if not currency:
            currency = detected_currency

        # Ratings and reviews (best-effort heuristics)
        rating_text = find_text(self.selector_sets["rating"]) or None
        review_text = find_text(self.selector_sets["reviews"]) or None
        rating_value = self._parse_rating(rating_text)
        review_count = self._parse_int(review_text)

        # Availability
        availability_text = find_text(self.selector_sets["availability"]) or None
        in_stock = self._infer_in_stock(availability_text)

        # Brand / SKU / Description
        brand = find_text(self.selector_sets["brand"]) or find_attr(self.selector_sets["brand"], "data-brand")

        sku = find_text(self.selector_sets["sku"]) or find_attr(self.selector_sets["sku"], "data-sku")
        if not sku:
            sku = find_attr(self.selector_sets["sku"], "data-product-sku")

        description = None
        for sel in self.selector_sets["description"]:
            try:
                el = card.find_element(By.CSS_SELECTOR, sel)
                desc = el.get_attribute("content") or el.text
                desc = self._clean_text(desc)
                if desc and len(desc) > 15:
                    description = desc[:400]
                    break
            except Exception:
                continue

        return {
            "title": title,
            "product_url": self._to_absolute(base_url, link_href) if link_href else None,
            "image_url": self._to_absolute(base_url, image_src) if image_src else None,
            "price": parsed_price,
            "currency": currency,
            "raw_price": raw_price,
            "rating": rating_value,
            "review_count": review_count,
            "in_stock": in_stock,
            "brand": brand,
            "sku": sku,
            "description": description,
        }

    # ----------------------------- JSON-LD Fallback ----------------------------

    def _extract_from_jsonld(self, driver: webdriver.Chrome, base_url: str, max_items: int) -> List[Dict[str, Any]]:
        products: List[Dict[str, Any]] = []
        scripts = driver.find_elements(By.XPATH, "//script[@type='application/ld+json']")
        for s in scripts:
            try:
                content = s.get_attribute("innerText") or ""
                blobs = self._safe_jsons_from_script(content)
                for blob in blobs:
                    self._collect_products_from_ldjson(blob, base_url, products, max_items)
            except Exception:
                continue
        return products[:max_items]

    def _collect_products_from_ldjson(self, data: Any, base_url: str, out: List[Dict[str, Any]], max_items: int):
        if len(out) >= max_items:
            return
        try:
            if isinstance(data, list):
                for item in data:
                    self._collect_products_from_ldjson(item, base_url, out, max_items)
            elif isinstance(data, dict):
                t = (data.get("@type") or data.get("type") or "").lower()
                if t in ["product", "listitem"] or "Product" in str(data.get("@type")):
                    product = self._map_ldjson_product(data, base_url)
                    if product and self._is_valid_product(product, base_url):
                        out.append(product)
                # Sometimes data is under itemListElement
                if "itemListElement" in data:
                    self._collect_products_from_ldjson(data["itemListElement"], base_url, out, max_items)
                if "mainEntity" in data:
                    self._collect_products_from_ldjson(data["mainEntity"], base_url, out, max_items)
        except Exception:
            return

    def _map_ldjson_product(self, d: Dict[str, Any], base_url: str) -> Optional[Dict[str, Any]]:
        name = d.get("name") or (d.get("item") or {}).get("name")
        url = d.get("url") or (d.get("item") or {}).get("url")
        image = d.get("image")
        if isinstance(image, list) and image:
            image = image[0]
        offers = d.get("offers") or {}
        if isinstance(offers, list) and offers:
            offers = offers[0]
        price = offers.get("price") if isinstance(offers, dict) else None
        currency = offers.get("priceCurrency") if isinstance(offers, dict) else None
        availability = offers.get("availability") if isinstance(offers, dict) else None
        agg_rating = d.get("aggregateRating") or {}
        rating_value = agg_rating.get("ratingValue") if isinstance(agg_rating, dict) else None
        review_count = agg_rating.get("reviewCount") if isinstance(agg_rating, dict) else None

        brand = d.get("brand")
        if isinstance(brand, dict):
            brand = brand.get("name") or brand.get("brand")
        elif isinstance(brand, list) and brand:
            first_brand = brand[0]
            if isinstance(first_brand, dict):
                brand = first_brand.get("name") or first_brand.get("brand")
            else:
                brand = first_brand

        sku = d.get("sku") or (d.get("item") or {}).get("sku")
        description = d.get("description") or (d.get("item") or {}).get("description")

        parsed_price, detected_currency = self._parse_price(str(price) if price is not None else None)
        if not currency:
            currency = detected_currency

        return {
            "title": self._clean_text(name),
            "product_url": self._to_absolute(base_url, url) if url else None,
            "image_url": self._to_absolute(base_url, image) if isinstance(image, str) else None,
            "price": parsed_price,
            "currency": currency,
            "raw_price": str(price) if price is not None else None,
            "rating": self._parse_float(rating_value),
            "review_count": self._parse_int(review_count),
            "in_stock": self._infer_in_stock(availability),
            "brand": self._clean_text(brand),
            "sku": self._clean_text(sku),
            "description": self._clean_text(description),
        }

    def _safe_jsons_from_script(self, content: str) -> List[Any]:
        blobs: List[Any] = []
        try:
            # Some sites embed multiple JSON objects or arrays; try naive splits
            candidates = [content]
            # Extract JSON-like blocks using braces/brackets balance heuristics
            # Fallback to raw parse if single block
            for cand in candidates:
                try:
                    parsed = json.loads(cand)
                    blobs.append(parsed)
                except Exception:
                    # Try to salvage arrays/objects inside
                    for match in re.findall(r"(\{.*?\}|\[.*?\])", cand, flags=re.DOTALL):
                        try:
                            blobs.append(json.loads(match))
                        except Exception:
                            continue
        except Exception:
            pass
        return blobs

    # ------------------------------- Utilities --------------------------------

    def _dismiss_known_popups(self, driver: webdriver.Chrome):
        for selector in self.popup_close_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements[:2]:
                    try:
                        if element.is_displayed():
                            driver.execute_script("arguments[0].click();", element)
                            time.sleep(0.3)
                    except Exception:
                        continue
            except Exception:
                continue

    def _progressive_scroll_and_load(self, driver: webdriver.Chrome):
        try:
            last_height = driver.execute_script("return document.body.scrollHeight")
        except Exception:
            last_height = 0

        for attempt in range(self.max_scroll_attempts):
            try:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            except Exception:
                break
            time.sleep(1.2)
            self._click_load_more(driver)
            self._dismiss_known_popups(driver)
            try:
                new_height = driver.execute_script("return document.body.scrollHeight")
            except Exception:
                break
            if new_height <= last_height:
                break
            last_height = new_height

    def _click_load_more(self, driver: webdriver.Chrome) -> bool:
        clicked = False
        for selector in self.load_more_selectors:
            try:
                buttons = driver.find_elements(By.CSS_SELECTOR, selector)
                for btn in buttons[:2]:
                    if btn.is_displayed() and btn.is_enabled():
                        try:
                            driver.execute_script("arguments[0].click();", btn)
                            time.sleep(1)
                            clicked = True
                        except Exception:
                            continue
            except Exception:
                continue
        return clicked

    def _setup_driver(self) -> webdriver.Chrome:
        chrome_options = Options()
        chrome_options.add_argument('--headless=new')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--disable-software-rasterizer')
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--disable-logging')
        chrome_options.add_argument('--disable-notifications')
        chrome_options.add_argument('--disable-default-apps')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"]) 
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        chrome_binary = os.getenv("CHROME_BIN")
        if chrome_binary:
            chrome_options.binary_location = chrome_binary

        driver_paths: List[str] = []
        env_driver_path = os.getenv("CHROMEDRIVER_PATH")
        if env_driver_path:
            driver_paths.append(env_driver_path)
        # Common fallback locations
        driver_paths.extend([
            "/usr/local/bin/chromedriver",
            "/usr/bin/chromedriver",
        ])

        last_error: Optional[Exception] = None
        for path in driver_paths:
            if not path or not os.path.exists(path):
                continue
            try:
                service = Service(path)
                driver = webdriver.Chrome(service=service, options=chrome_options)
                driver.set_page_load_timeout(30)
                return driver
            except Exception as exc:
                last_error = exc
                print(f"[!] Failed to start Chrome using system driver at {path}: {exc}")

        if WEBDRIVER_MANAGER_AVAILABLE:
            try:
                service = Service(ChromeDriverManager().install())
                driver = webdriver.Chrome(service=service, options=chrome_options)
                driver.set_page_load_timeout(30)
                return driver
            except Exception as exc:
                last_error = exc
                print(f"[!] WebDriver Manager failed to obtain driver: {exc}")

        try:
            driver = webdriver.Chrome(options=chrome_options)
            driver.set_page_load_timeout(30)
            return driver
        except Exception as exc:
            if last_error:
                raise last_error
            raise exc

    def _wait_for_any_selector(self, driver: webdriver.Chrome, selectors: List[str], wait_seconds: int):
        end = time.time() + wait_seconds
        while time.time() < end:
            for sel in selectors:
                try:
                    els = driver.find_elements(By.CSS_SELECTOR, sel)
                    visible = [e for e in els if e.is_displayed()]
                    if visible:
                        return
                except Exception:
                    continue
            time.sleep(0.25)
        # Soft timeout only; DOM extraction still attempts heuristics

    def _find_first_nonempty_set(self, driver: webdriver.Chrome, selectors: List[str], by: By):
        for sel in selectors:
            try:
                els = driver.find_elements(by, sel)
                els = [e for e in els if e.is_displayed()]
                if els:
                    return els
            except Exception:
                continue
        return []

    def _page_indicates_no_results(self, driver: webdriver.Chrome) -> bool:
        try:
            body_text = (driver.find_element(By.TAG_NAME, 'body').text or '').lower()
            return any(p in body_text for p in self.no_results_phrases)
        except Exception:
            return False

    # ------------------------ Structured Data Strategies -----------------------

    def _extract_from_microdata(self, driver: webdriver.Chrome, base_url: str, max_items: int) -> List[Dict[str, Any]]:
        products: List[Dict[str, Any]] = []
        try:
            nodes = driver.find_elements(By.CSS_SELECTOR, '[itemscope][itemtype*="Product" i]')
        except Exception:
            nodes = []

        for node in nodes:
            try:
                if self._is_within_blacklisted_section(node):
                    continue
            except Exception:
                pass
            try:
                product = self._extract_microdata_node(node, base_url)
                if product and self._is_valid_product(product, base_url):
                    products.append(product)
                    if len(products) >= max_items:
                        break
            except Exception:
                continue

        return products

    def _extract_microdata_node(self, node, base_url: str) -> Optional[Dict[str, Any]]:
        data: Dict[str, Any] = {}

        # Direct attributes on the node
        try:
            itemid = node.get_attribute('itemid')
            if itemid and not data.get('product_url'):
                data['product_url'] = self._to_absolute(base_url, itemid)
        except Exception:
            pass

        props = []
        try:
            props = node.find_elements(By.CSS_SELECTOR, '[itemprop]')
        except Exception:
            props = []

        for prop in props:
            try:
                key = prop.get_attribute('itemprop')
                if not key:
                    continue
                key = key.lower()
                value = (
                    prop.get_attribute('content')
                    or prop.get_attribute('href')
                    or prop.get_attribute('src')
                    or prop.text
                )
                value = self._clean_text(value)

                # Nested brand/item scopes
                if key == 'brand' and (not value or len(value) <= 2):
                    try:
                        nested_name = prop.find_element(By.CSS_SELECTOR, '[itemprop="name"]')
                        value = self._clean_text(
                            nested_name.get_attribute('content') or nested_name.text
                        )
                    except Exception:
                        pass

                if key == 'name' and value and not data.get('title'):
                    data['title'] = value
                elif key in ('url', 'link') and value and not data.get('product_url'):
                    data['product_url'] = self._to_absolute(base_url, value)
                elif key == 'image' and value and not data.get('image_url'):
                    data['image_url'] = self._to_absolute(base_url, value)
                elif key == 'price':
                    data['raw_price'] = value
                elif key in ('pricecurrency', 'currency') and value:
                    data['currency'] = value
                elif key == 'availability' and value:
                    data['availability'] = value
                elif key == 'description' and value:
                    data['description'] = value[:400]
                elif key == 'brand' and value and not data.get('brand'):
                    data['brand'] = value
                elif key == 'sku' and value and not data.get('sku'):
                    data['sku'] = value
                elif key == 'ratingvalue' and value:
                    data['rating'] = value
                elif key in ('reviewcount', 'ratingcount') and value:
                    data['review_count'] = value
            except Exception:
                continue

        parsed_price, detected_currency = self._parse_price(data.get('raw_price'))
        currency = data.get('currency') or detected_currency

        return {
            'title': self._clean_text(data.get('title')),
            'product_url': data.get('product_url'),
            'image_url': data.get('image_url'),
            'price': parsed_price,
            'currency': self._clean_text(currency),
            'raw_price': data.get('raw_price'),
            'rating': self._parse_float(data.get('rating')),
            'review_count': self._parse_int(data.get('review_count')),
            'in_stock': self._infer_in_stock(data.get('availability')),
            'brand': self._clean_text(data.get('brand')),
            'sku': self._clean_text(data.get('sku')),
            'description': self._clean_text(data.get('description')),
        }

    def _extract_from_inline_data_scripts(self, driver: webdriver.Chrome, base_url: str, max_items: int) -> List[Dict[str, Any]]:
        products: List[Dict[str, Any]] = []
        scripts = driver.find_elements(By.XPATH, "//script[@type='application/json' or @type='text/json' or @type='text/plain']")
        for s in scripts:
            try:
                raw = s.get_attribute('innerText') or ''
                if not raw:
                    continue
                if len(raw) > 500_000:
                    continue  # avoid huge blobs
                blobs = self._safe_jsons_from_script(raw)
                for blob in blobs:
                    self._collect_products_from_generic_json(blob, base_url, products, max_items)
                    if len(products) >= max_items:
                        return products
            except Exception:
                continue
        return products

    def _collect_products_from_generic_json(self, data: Any, base_url: str, out: List[Dict[str, Any]], max_items: int, depth: int = 0):
        if len(out) >= max_items or depth > 6:
            return
        try:
            if isinstance(data, list):
                for item in data:
                    self._collect_products_from_generic_json(item, base_url, out, max_items, depth + 1)
                    if len(out) >= max_items:
                        break
            elif isinstance(data, dict):
                product = self._map_generic_json_product(data, base_url)
                if product and self._is_valid_product(product, base_url):
                    out.append(product)
                    if len(out) >= max_items:
                        return

                for key, value in data.items():
                    if isinstance(value, (list, dict)):
                        key_lower = str(key).lower()
                        if any(k in key_lower for k in ['product', 'item', 'sku', 'listing', 'result', 'entries', 'records']):
                            self._collect_products_from_generic_json(value, base_url, out, max_items, depth + 1)
                        elif depth <= 1:
                            # Explore shallow keys even if they don't look product-like
                            self._collect_products_from_generic_json(value, base_url, out, max_items, depth + 1)
        except Exception:
            return

    def _map_generic_json_product(self, data: Dict[str, Any], base_url: str) -> Optional[Dict[str, Any]]:
        if not isinstance(data, dict):
            return None

        def extract_first(keys: List[str]):
            for key in keys:
                if key in data and data[key] not in (None, ''):
                    val = data[key]
                    if isinstance(val, list):
                        return val[0]
                    return val
            return None

        title = extract_first(['name', 'title', 'productName', 'product_name', 'label'])
        url = extract_first(['url', 'link', 'productUrl', 'productURL', 'href', 'canonicalUrl'])
        image = extract_first(['image', 'imageUrl', 'imageURL', 'thumbnail', 'thumbnailUrl', 'mediaUrl', 'picture'])
        raw_price = extract_first(['price', 'salePrice', 'offerPrice', 'priceValue', 'price_amount', 'priceWithTax'])
        currency = extract_first(['currency', 'currencyCode', 'priceCurrency'])
        brand = extract_first(['brand', 'manufacturer', 'maker'])
        sku = extract_first(['sku', 'id', 'productId', 'product_id', 'itemId'])
        description = extract_first(['description', 'shortDescription', 'summary'])
        rating = extract_first(['rating', 'ratingValue', 'averageRating', 'reviewRating'])
        review_count = extract_first(['reviewCount', 'reviewsCount', 'numberOfReviews', 'ratingCount'])
        availability = extract_first(['availability', 'stockStatus', 'availabilityStatus'])

        # Nested price dicts
        if isinstance(raw_price, dict):
            raw_price = raw_price.get('value') or raw_price.get('amount') or raw_price.get('price')

        if isinstance(url, dict):
            url = url.get('url') or url.get('href')

        if isinstance(image, dict):
            image = image.get('url') or image.get('src')

        parsed_price, detected_currency = self._parse_price(str(raw_price) if raw_price is not None else None)
        if not currency:
            currency = detected_currency

        product = {
            'title': self._clean_text(title),
            'product_url': self._to_absolute(base_url, url) if url else None,
            'image_url': self._to_absolute(base_url, image) if isinstance(image, str) else None,
            'price': parsed_price,
            'currency': self._clean_text(currency),
            'raw_price': str(raw_price) if raw_price is not None else None,
            'rating': self._parse_float(rating),
            'review_count': self._parse_int(review_count),
            'in_stock': self._infer_in_stock(availability),
            'brand': self._clean_text(brand),
            'sku': self._clean_text(sku),
            'description': self._clean_text(description),
        }

        if not product.get('title') and not product.get('product_url'):
            return None
        return product

    # -------------------------- Additional Strategies --------------------------

    def _extract_by_global_heuristics(self, driver: webdriver.Chrome, base_url: str, max_items: int) -> List[Dict[str, Any]]:
        products: List[Dict[str, Any]] = []
        # Avoid header/footer/nav/aside
        candidates = driver.find_elements(By.CSS_SELECTOR, "main, section, div")
        candidates = [c for c in candidates if c.is_displayed()]
        for cont in candidates:
            try:
                if self._is_within_blacklisted_section(cont):
                    continue
                cards = cont.find_elements(By.CSS_SELECTOR, 'li, div, article')
                for card in cards:
                    if not card.is_displayed():
                        continue
                    if self._is_within_blacklisted_section(card):
                        continue
                    if not self._looks_like_product_card(card):
                        continue
                    product = self._extract_fields_from_card(card, base_url)
                    if product and self._is_valid_product(product, base_url):
                        products.append(product)
                        if len(products) >= max_items:
                            return products
            except Exception:
                continue
        return products

    def _extract_from_links_with_images(self, driver: webdriver.Chrome, base_url: str, max_items: int) -> List[Dict[str, Any]]:
        products: List[Dict[str, Any]] = []
        anchors = driver.find_elements(By.CSS_SELECTOR, 'a[href]')
        for a in anchors:
            try:
                if not a.is_displayed():
                    continue
                if self._is_within_blacklisted_section(a):
                    continue
                href = a.get_attribute('href')
                if not self._is_potential_product_href(href, base_url):
                    continue
                # Require image in the anchor or immediate container
                has_img = False
                image_el = None
                try:
                    image_el = a.find_element(By.CSS_SELECTOR, 'img[src], img[data-src], img[data-original], img[data-srcset]')
                    has_img = True
                except Exception:
                    try:
                        parent = a.find_element(By.XPATH, './..')
                        image_el = parent.find_element(By.CSS_SELECTOR, 'img[src], img[data-src], img[data-original], img[data-srcset]')
                        has_img = True
                    except Exception:
                        pass
                if not has_img:
                    continue
                title = self._clean_text(a.get_attribute('title') or a.text)
                image_url = None
                if image_el:
                    image_url = (
                        image_el.get_attribute('src')
                        or image_el.get_attribute('data-src')
                        or image_el.get_attribute('data-original')
                        or image_el.get_attribute('data-srcset')
                    )
                product = {
                    'title': title,
                    'product_url': self._to_absolute(base_url, href),
                    'image_url': self._to_absolute(base_url, image_url) if image_url else None,
                    'price': None,
                    'currency': None,
                    'raw_price': None,
                    'rating': None,
                    'review_count': None,
                    'in_stock': None,
                    'brand': None,
                    'sku': None,
                    'description': None,
                }
                if self._is_valid_product(product, base_url):
                    products.append(product)
                    if len(products) >= max_items:
                        break
            except Exception:
                continue
        return products

    # ------------------------------ Validations -------------------------------

    def _is_within_blacklisted_section(self, element) -> bool:
        if element is None:
            return False
        try:
            current = element
            for _ in range(6):
                tag = current.tag_name.lower()
                if tag in self.blacklisted_sections:
                    return True
                if tag in ('body', 'html'):
                    break
                parent = current.find_element(By.XPATH, '..')
                if parent is current:
                    break
                current = parent
        except Exception:
            return False
        return False

    def _is_valid_product(self, product: Dict[str, Any], base_url: str) -> bool:
        url = product.get('product_url')
        title = self._clean_text(product.get('title')) if product.get('title') else None

        if not url:
            return False
        if self._is_blacklisted_link(url):
            return False
        if not self._is_product_like_path(url, base_url) and not (product.get('price') and title):
            return False
        if title and (self._looks_like_phone_or_nav(title) or len(title) < 2):
            return False
        if not title and not product.get('price') and not product.get('raw_price'):
            return False
        return True

    def _is_blacklisted_link(self, href: str) -> bool:
        if not href:
            return True
        h = href.lower()
        if any(h.startswith(prefix) for prefix in ('javascript:', 'mailto:', 'tel:')):
            return True
        return any(keyword in h for keyword in self.link_blacklist_keywords)

    def _is_product_like_path(self, href: str, base_url: str) -> bool:
        try:
            parsed = urlparse(href)
            path = (parsed.path or '').lower()
            query = (parsed.query or '').lower()
            fragment = (parsed.fragment or '').lower()

            if path in ('', '/', '/home', '/index', '/index.html'):
                return False

            combined = f"{path}?{query}#{fragment}"
            if any(keyword in combined for keyword in self.product_path_keywords):
                return True

            negative_keywords = ['search', 'account', 'contact', 'login', 'register', 'wishlist', 'cart', 'help', 'support', 'faq', 'privacy', 'terms']
            if any(neg in combined for neg in negative_keywords):
                return False

            if path.endswith('.html') or path.endswith('.htm'):
                return True
            if path.count('/') >= 2 and len(path) > 3:
                return True
            if '-' in path and len(path.replace('-', '')) > 6:
                return True
            return False
        except Exception:
            return False

    def _is_potential_product_href(self, href: Optional[str], base_url: str) -> bool:
        if not href:
            return False
        if self._is_blacklisted_link(href):
            return False
        return self._is_product_like_path(href, base_url)

    def _looks_like_phone_or_nav(self, text: str) -> bool:
        if not text:
            return False
        t = text.lower()
        if re.search(r"\b\+?\d{8,}\b", t):  # long phone numbers
            return True
        nav_words = [
            'home', 'about', 'contact', 'help', 'account', 'login', 'register', 'signup',
            'wishlist', 'cart', 'track', 'order', 'policy', 'privacy', 'terms', 'faq',
            'support', 'customer care', 'service', 'blog', 'news', 'store locator'
        ]
        return any(n in t for n in nav_words)

    def _extract_price_from_text(self, text: Optional[str]) -> Optional[str]:
        if not text:
            return None
        t = text.strip()
        m = re.search(
            r"((?:₹|rs\.?|rs\s|inr\s|usd\s|eur\s|cad\s|aud\s|£|€|\$)\s*[\d,.]+(?:\.\d{1,2})?)",
            t,
            flags=re.IGNORECASE,
        )
        return m.group(1) if m else None

    def _dedupe_by_url(self, products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        aggregated: Dict[str, Dict[str, Any]] = {}
        order: List[str] = []
        for p in products:
            url = p.get('product_url')
            if not url:
                continue
            if url not in aggregated:
                aggregated[url] = dict(p)
                order.append(url)
            else:
                existing = aggregated[url]
                for key, value in p.items():
                    if key == '_element':
                        continue
                    if value and not existing.get(key):
                        existing[key] = value
        return [aggregated[u] for u in order]

    def _clean_text(self, text: Optional[str]) -> Optional[str]:
        if text is None:
            return None
        cleaned = re.sub(r"\s+", " ", text).strip()
        return cleaned or None

    def _parse_price(self, raw: Optional[str]) -> (Optional[float], Optional[str]):
        if not raw:
            return None, None
        txt = raw.strip()
        currency = None
        # Detect common currency symbols/keywords
        lowered = txt.lower()
        if any(sym in lowered for sym in ["₹", "rs", "rs.", "inr"]):
            currency = "INR"
        elif "$" in txt or "usd" in lowered:
            currency = "USD"
        elif "€" in txt or "eur" in lowered:
            currency = "EUR"
        elif "£" in txt or "gbp" in lowered:
            currency = "GBP"
        elif "cad" in lowered:
            currency = "CAD"
        elif "aud" in lowered:
            currency = "AUD"
        # Extract number
        num_match = re.findall(r"[\d,.]+", txt)
        if not num_match:
            return None, currency
        num = num_match[0].replace(",", "")
        try:
            return float(num), currency
        except Exception:
            return None, currency

    def _parse_rating(self, raw: Optional[str]) -> Optional[float]:
        return self._parse_float(raw)

    def _parse_int(self, raw: Optional[str]) -> Optional[int]:
        if not raw:
            return None
        m = re.findall(r"\d+", str(raw))
        if not m:
            return None
        try:
            return int(m[0])
        except Exception:
            return None

    def _parse_float(self, raw: Optional[str]) -> Optional[float]:
        if not raw:
            return None
        m = re.findall(r"[\d.]+", str(raw))
        if not m:
            return None
        try:
            return float(m[0])
        except Exception:
            return None

    def _infer_in_stock(self, availability_text: Optional[str]) -> Optional[bool]:
        if availability_text is None:
            return None
        t = availability_text.lower()
        if any(k in t for k in ["in stock", "instock", "available", "availabilityinstock"]):
            return True
        if any(k in t for k in ["out of stock", "outofstock", "unavailable"]):
            return False
        return None

    def _to_absolute(self, base_url: str, href: Optional[str]) -> Optional[str]:
        if not href:
            return None
        try:
            return urljoin(base_url, href)
        except Exception:
            return href

    # ----------------------------- Database Operations -----------------------------

    def _save_products_to_db(self, products: List[Dict[str, Any]], platform_url: str, platform: str,
                           product_type_id: Optional[int] = None, searched_product_id: Optional[int] = None) -> int:
        """
        Save extracted products to the r_product_data table in Supabase
        
        Args:
            products: List of product dictionaries
            platform_url: URL of the platform/page where products were extracted
            platform: Platform domain name
            product_type_id: ID of the product type from product_type_table (optional)
            searched_product_id: ID of the product from products table that was searched for (optional)
            
        Returns:
            Number of products successfully saved
        """
        if not self.supabase:
            print("[!] Supabase not available - products not saved to database")
            return 0
        
        if not products:
            print("[!] No products to save")
            return 0
        
        saved_count = 0
        failed_count = 0
        
        print(f"\n[*] Saving {len(products)} products to database...")
        
        for product in products:
            try:
                # Validate and sanitize rating (must be between 0 and 100)
                rating = product.get("rating")
                if rating is not None:
                    try:
                        rating_float = float(rating)
                        # Clamp rating between 0 and 100 (some sites use 0-10, some 0-5, some 0-100)
                        if rating_float < 0:
                            rating = 0.0
                        elif rating_float > 100:
                            rating = 100.0
                        else:
                            rating = round(rating_float, 2)
                    except (ValueError, TypeError):
                        rating = None
                
                # Validate and sanitize price
                price = product.get("price")
                if price is not None:
                    try:
                        price_float = float(price)
                        # Ensure price is positive and reasonable (max 999999999.99)
                        if price_float < 0:
                            price = None
                        elif price_float > 999999999.99:
                            price = 999999999.99
                        else:
                            price = round(price_float, 2)
                    except (ValueError, TypeError):
                        price = None
                
                # Validate reviews count (must be positive integer)
                reviews = product.get("review_count")
                if reviews is not None:
                    try:
                        reviews_int = int(float(reviews))  # Handle float strings
                        if reviews_int < 0:
                            reviews = None
                        else:
                            reviews = reviews_int
                    except (ValueError, TypeError):
                        reviews = None
                
                # Map extracted fields to database fields
                db_data = {
                    "platform_url": platform_url,
                    "product_name": product.get("title") or "",
                    "original_price": product.get("raw_price"),  # Keep as text for display
                    "current_price": price,
                    "product_url": product.get("product_url") or "",
                    "product_image_url": product.get("image_url"),
                    "description": product.get("description"),
                    "rating": rating,
                    "reviews": reviews,
                    "in_stock": product.get("in_stock"),
                    "brand": product.get("brand"),
                    "product_type_id": product_type_id,
                    "searched_product_id": searched_product_id,
                }
                
                # Skip if required fields are missing
                if not db_data["product_name"] or not db_data["product_url"]:
                    print(f"[!] Skipping product - missing required fields (name or URL)")
                    failed_count += 1
                    continue
                
                # Insert into database
                # If product_url has unique constraint, duplicates will be handled by database
                response = self.supabase.table("r_product_data").insert(db_data).execute()
                
                if response.data:
                    saved_count += 1
                    if saved_count % 10 == 0:
                        print(f"[*] Saved {saved_count} products so far...")
                else:
                    failed_count += 1
                    
            except Exception as e:
                error_msg = str(e).lower()
                # Handle duplicate key errors gracefully (if product_url has unique constraint)
                if "duplicate" in error_msg or "unique" in error_msg or "constraint" in error_msg:
                    # Product already exists, skip silently
                    saved_count += 1  # Count as successful since product already exists
                else:
                    print(f"[✗] Error saving product: {e}")
                    failed_count += 1
                continue
        
        print(f"[✓] Saved {saved_count}/{len(products)} products to database")
        if failed_count > 0:
            print(f"[!] Failed to save {failed_count} products")
        
        return saved_count


# ============================================================================
# Parallel execution helpers
# ============================================================================


class ParallelURLExtractor:
    """Run the universal extractor against many URLs concurrently."""

    def __init__(
        self,
        max_workers: Optional[int] = None,
        default_wait_seconds: int = 12,
        default_max_items: int = 50,
    ):
        self.max_workers = _determine_parallel_workers(max_workers)
        db_batch_size = _get_env_int("DB_URL_BATCH_SIZE", 1000)
        self.batch_size = max(1, db_batch_size)
        self.default_wait_seconds = default_wait_seconds
        self.default_max_items = default_max_items
        self.max_retries = _get_env_int("MAX_RETRIES", 3)

        self._executor = ThreadPoolExecutor(max_workers=self.max_workers)
        self._thread_local = threading.local()
        self._extractors_lock = threading.Lock()
        self._extractors: List[UniversalProductExtractor] = []
        self._pending = 0
        self._pending_lock = threading.Lock()
        self._shutdown = False

    # ------------------------------------------------------------------
    # Context management & lifecycle
    # ------------------------------------------------------------------

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.shutdown()

    def __del__(self):
        try:
            self.shutdown()
        except Exception:
            pass

    def shutdown(self, wait: bool = True):
        if self._shutdown:
            return
        self._shutdown = True
        try:
            self._executor.shutdown(wait=wait)
        finally:
            with self._extractors_lock:
                extractors = list(self._extractors)
                self._extractors.clear()
            for extractor in extractors:
                try:
                    extractor.shutdown()
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # Worker helpers
    # ------------------------------------------------------------------

    def _get_extractor(self) -> UniversalProductExtractor:
        extractor = getattr(self._thread_local, "extractor", None)
        if extractor is None:
            extractor = UniversalProductExtractor()
            self._thread_local.extractor = extractor
            with self._extractors_lock:
                self._extractors.append(extractor)
        return extractor

    def pending_count(self) -> int:
        with self._pending_lock:
            return self._pending

    # ------------------------------------------------------------------
    # Job execution
    # ------------------------------------------------------------------

    def _normalize_job(
        self,
        entry: Union[str, Dict[str, Any]],
        max_items_override: Optional[int],
        wait_seconds_override: Optional[int],
    ) -> Dict[str, Any]:
        if isinstance(entry, str):
            job: Dict[str, Any] = {"url": entry}
        elif isinstance(entry, dict):
            if "url" not in entry:
                raise ValueError("URL job dictionaries must include a 'url' key")
            job = dict(entry)
        else:
            raise TypeError("Each URL entry must be a string or a dict with a 'url' key")

        job.setdefault("max_items", max_items_override or self.default_max_items)
        job.setdefault("wait_seconds", wait_seconds_override or self.default_wait_seconds)
        job.setdefault("max_retries", self.max_retries)
        job.setdefault("retry_count", 0)
        if "url_id" not in job and "id" in job:
            job["url_id"] = job["id"]

        return job

    def _run_job(self, job: Dict[str, Any]) -> Dict[str, Any]:
        extractor = self._get_extractor()
        with self._pending_lock:
            self._pending += 1

        start_time = time.time()
        url_id = job.get("url_id")
        retry_count = job.get("retry_count", 0) or 0
        attempt_count = retry_count + 1
        max_retries = job.get("max_retries", 0) or 0
        error_message: Optional[str] = None
        result: Dict[str, Any]
        try:
            result = extractor.extract_products(
                job["url"],
                max_items=job.get("max_items", self.default_max_items),
                wait_seconds=job.get("wait_seconds", self.default_wait_seconds),
                product_type_id=job.get("product_type_id"),
                searched_product_id=job.get("searched_product_id"),
                reuse_driver=True,
                url_id=url_id,
            )
        except Exception as exc:
            extractor.close_reusable_driver()
            duration = time.time() - start_time
            error_message = str(exc)
            if url_id is not None:
                _mark_for_retry(url_id, retry_count, error_message, max_retries)
            return {
                "success": False,
                "page_url": job["url"],
                "url": job["url"],
                "error": error_message,
                "duration_seconds": round(duration, 3),
                "url_id": url_id,
            }
        finally:
            with self._pending_lock:
                self._pending -= 1

        duration = time.time() - start_time
        result.setdefault("page_url", job["url"])
        result.setdefault("url", job["url"])
        result["duration_seconds"] = round(duration, 3)
        if "job" not in result:
            result["job"] = job
        result["url_id"] = url_id

        if url_id is not None:
            if result.get("success"):
                _update_url_status(
                    url_id,
                    processing_status="completed",
                    success=True,
                    products_found=result.get("num_products"),
                    products_saved=result.get("saved_to_db"),
                    error_message=None,
                    retry_count=attempt_count,
                    clear_claim=True,
                )
            else:
                error_message = result.get("error")
                if _should_retry(attempt_count, max_retries):
                    _mark_for_retry(url_id, retry_count, error_message, max_retries)
                    result["retry_scheduled"] = True
                else:
                    _update_url_status(
                        url_id,
                        processing_status="failed",
                        success=False,
                        products_found=result.get("num_products"),
                        products_saved=result.get("saved_to_db"),
                        error_message=error_message,
                        retry_count=attempt_count,
                        clear_claim=True,
                    )

        return result

    def run_bulk(
        self,
        urls: Iterable[Union[str, Dict[str, Any]]],
        max_items: Optional[int] = None,
        wait_seconds: Optional[int] = None,
        progress_callback: Optional[Any] = None,
    ) -> Dict[str, Any]:
        jobs = [self._normalize_job(entry, max_items, wait_seconds) for entry in urls]
        total_jobs = len(jobs)
        if total_jobs == 0:
            return {
                "stats": {
                    "submitted": 0,
                    "succeeded": 0,
                    "failed": 0,
                    "total_products_found": 0,
                    "total_saved_to_db": 0,
                    "duration_seconds": 0.0,
                },
                "results": [],
            }

        overall_start = time.time()
        results: List[Dict[str, Any]] = []
        stats = {
            "submitted": total_jobs,
            "succeeded": 0,
            "failed": 0,
            "total_products_found": 0,
            "total_saved_to_db": 0,
        }

        batch_size = self.batch_size if self.batch_size and self.batch_size > 0 else total_jobs
        for batch_start in range(0, total_jobs, batch_size):
            batch = jobs[batch_start: batch_start + batch_size]
            futures = [self._executor.submit(self._run_job, job) for job in batch]
            for future in as_completed(futures):
                result = future.result()
                results.append(result)

                if result.get("success"):
                    stats["succeeded"] += 1
                    stats["total_products_found"] += result.get("num_products", 0) or 0
                    stats["total_saved_to_db"] += result.get("saved_to_db", 0) or 0
                else:
                    stats["failed"] += 1

                if progress_callback:
                    try:
                        progress_callback(result, dict(stats))
                    except Exception:
                        pass

        stats["duration_seconds"] = round(time.time() - overall_start, 2)
        return {"stats": stats, "results": results}

    def dry_run(
        self,
        urls: Iterable[Union[str, Dict[str, Any]]],
        sample_size: int = 3,
        **kwargs,
    ) -> Dict[str, Any]:
        url_list = list(urls)
        sample = url_list[:sample_size]
        if not sample:
            return {
                "stats": {"submitted": 0, "succeeded": 0, "failed": 0, "total_products_found": 0, "total_saved_to_db": 0, "duration_seconds": 0.0},
                "results": [],
            }
        return self.run_bulk(sample, **kwargs)


# ============================================================================
# Helper utilities for CLI usage
# ============================================================================


def _parse_url_payload(payload: str) -> List[Union[str, Dict[str, Any]]]:
    payload = payload.strip()
    if not payload:
        return []

    try:
        parsed = json.loads(payload)
        if isinstance(parsed, list):
            return [item for item in parsed if isinstance(item, (str, dict))]
        if isinstance(parsed, dict) and "urls" in parsed:
            urls_value = parsed["urls"]
            if isinstance(urls_value, list):
                return [item for item in urls_value if isinstance(item, (str, dict))]
        if isinstance(parsed, str):
            return [parsed]
    except json.JSONDecodeError:
        pass

    urls: List[Union[str, Dict[str, Any]]] = []
    raw_lines = payload.splitlines()
    if len(raw_lines) == 1 and "," in payload and not payload.strip().startswith("{"):
        raw_lines = [part.strip() for part in payload.split(",")]

    for line in raw_lines:
        candidate = line.strip()
        if not candidate or candidate.startswith("#"):
            continue
        try:
            decoded = json.loads(candidate)
            if isinstance(decoded, dict) and "url" in decoded:
                urls.append(decoded)
                continue
            if isinstance(decoded, str):
                urls.append(decoded)
                continue
        except json.JSONDecodeError:
            pass
        urls.append(candidate)
    return urls


def _load_bulk_urls_from_env() -> List[Union[str, Dict[str, Any]]]:
    urls: List[Union[str, Dict[str, Any]]] = []
    env_payload = os.getenv("BULK_URLS")
    if env_payload:
        urls.extend(_parse_url_payload(env_payload))

    file_path = os.getenv("BULK_URLS_FILE")
    if file_path:
        if os.path.exists(file_path):
            with open(file_path, "r", encoding="utf-8") as handle:
                payload = handle.read()
            urls.extend(_parse_url_payload(payload))
        else:
            print(f"[!] BULK_URLS_FILE not found: {file_path}")

    return urls


def _print_bulk_summary(summary: Dict[str, Any], sample_limit: int = 5) -> None:
    stats = summary.get("stats", {})
    print("\n" + "=" * 80)
    print("BULK EXTRACTION SUMMARY")
    print("=" * 80)
    print(f"Submitted URLs     : {stats.get('submitted', 0)}")
    print(f"Succeeded          : {stats.get('succeeded', 0)}")
    print(f"Failed             : {stats.get('failed', 0)}")
    print(f"Products extracted : {stats.get('total_products_found', 0)}")
    print(f"Products saved     : {stats.get('total_saved_to_db', 0)}")
    print(f"Duration (s)       : {stats.get('duration_seconds', 0.0)}")

    results = summary.get("results", [])
    if not results:
        print("[!] No results to display")
        return

    successful = [r for r in results if r.get("success")]
    failed = [r for r in results if not r.get("success")]

    if successful:
        print("\nTop successful URLs:")
        for entry in successful[:sample_limit]:
            print(
                f"  ✓ {entry.get('url')} → {entry.get('num_products', 0)} products "
                f"(saved {entry.get('saved_to_db', 0)})"
            )
    if failed:
        print("\nSample failures:")
        for entry in failed[:sample_limit]:
            print(f"  ✗ {entry.get('url')} → {entry.get('error', 'unknown error')}")


def _parse_bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _get_env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _estimate_ram_gb() -> float:
    try:
        import psutil  # type: ignore

        return psutil.virtual_memory().total / (1024 ** 3)
    except Exception:
        pass
    for candidate in ("SYSTEM_RAM_GB", "RAM_GB", "AVAILABLE_RAM_GB"):
        raw = os.getenv(candidate)
        if raw:
            try:
                return float(raw)
            except ValueError:
                continue
    return 8.0


def _determine_parallel_workers(explicit_workers: Optional[int] = None) -> int:
    if explicit_workers and explicit_workers > 0:
        return explicit_workers
    env_workers = _get_env_int("MAX_PARALLEL_WORKERS", 0)
    if env_workers > 0:
        return env_workers
    cpu_count = os.cpu_count() or 4
    ram_gb = _estimate_ram_gb()
    # Assume each headless Chrome instance consumes roughly 0.5GB.
    ram_limited = max(1, int(ram_gb / 0.5))
    # Allow up to 4x CPU cores but respect RAM limit.
    cpu_limited = max(1, cpu_count * 4)
    return max(1, min(ram_limited, cpu_limited, 250))


def _parse_status_filters(raw: Optional[str]) -> List[str]:
    if not raw:
        return ["pending", "retrying"]
    filters = [token.strip().lower() for token in raw.split(",") if token.strip()]
    return filters or ["pending", "retrying"]


def _process_url_batches(
    status_filters: List[str],
    db_limit: int,
    db_offset: int,
    dry_run_size: int,
    only_dry_run: bool,
    progress_enabled: bool,
) -> None:
    client = _get_supabase_client()
    if not client:
        print("[✗] Supabase client is required for database-driven extraction.")
        return

    overall_start = time.time()
    aggregated_results: List[Dict[str, Any]] = []
    aggregated_stats = {
        "submitted": 0,
        "succeeded": 0,
        "failed": 0,
        "total_products_found": 0,
        "total_saved_to_db": 0,
    }

    target_limit = dry_run_size if dry_run_size > 0 else db_limit
    processed_count = 0
    worker_prefix = str(uuid.uuid4())[:8]
    batch_index = 0
    min_id: Optional[int] = None

    if db_offset > 0:
        anchor_rows = _load_urls_from_database(limit=1, offset=db_offset, status_filters=status_filters)
        if anchor_rows:
            min_id = anchor_rows[0].get("id")
            print(f"[*] Starting from URL id >= {min_id} based on DB_URL_OFFSET={db_offset}")
        else:
            print(f"[!] Unable to locate URL at offset {db_offset}; processing from beginning.")

    with ParallelURLExtractor() as runner:
        effective_limit = target_limit if target_limit and target_limit > 0 else None

        def _progress_callback(result: Dict[str, Any], stats_snapshot: Dict[str, Any]):
            processed = stats_snapshot.get("succeeded", 0) + stats_snapshot.get("failed", 0)
            total = stats_snapshot.get("submitted", 0)
            status = "✓" if result.get("success") else "✗"
            message = (
                f"{result.get('num_products', 0)} products"
                if result.get("success")
                else result.get("error", "error")
            )
            print(f"[{status}] ({processed}/{total}) {result.get('url')} → {message}")

        while True:
            if effective_limit is not None and processed_count >= effective_limit:
                break

            remaining = (
                effective_limit - processed_count if effective_limit is not None else runner.batch_size
            )
            batch_size = min(runner.batch_size, remaining) if effective_limit is not None else runner.batch_size
            if batch_size <= 0:
                break

            claimed_rows, worker_id = _claim_urls_batch(
                batch_size,
                status_filters=status_filters,
                worker_id=f"{worker_prefix}-{batch_index}",
                min_id=min_id,
            )
            if not claimed_rows:
                if batch_index == 0:
                    print(f"[*] No URLs available with statuses {status_filters}")
                break

            jobs: List[Dict[str, Any]] = []
            for row in claimed_rows:
                if effective_limit is not None and processed_count + len(jobs) >= effective_limit:
                    break
                jobs.append(
                    {
                        "url": row.get("product_page_url"),
                        "url_id": row.get("id"),
                        "retry_count": row.get("retry_count") or 0,
                        "max_retries": runner.max_retries,
                        "product_type_id": row.get("product_type_id"),
                    }
                )

            if not jobs:
                break

            batch_summary = runner.run_bulk(
                jobs,
                progress_callback=_progress_callback if progress_enabled else None,
            )

            processed_count += batch_summary["stats"]["submitted"]
            aggregated_stats["submitted"] += batch_summary["stats"]["submitted"]
            aggregated_stats["succeeded"] += batch_summary["stats"]["succeeded"]
            aggregated_stats["failed"] += batch_summary["stats"]["failed"]
            aggregated_stats["total_products_found"] += batch_summary["stats"]["total_products_found"]
            aggregated_stats["total_saved_to_db"] += batch_summary["stats"]["total_saved_to_db"]
            aggregated_results.extend(batch_summary["results"])

            batch_index += 1

            if only_dry_run:
                print("\n[!] DRY_RUN_ONLY enabled. Processed sample batch and exiting.")
                break

    aggregated_stats["duration_seconds"] = round(time.time() - overall_start, 2)
    summary_payload = {"stats": aggregated_stats, "results": aggregated_results}
    _print_bulk_summary(summary_payload)


def _claim_urls_batch(
    batch_size: int,
    status_filters: Optional[List[str]] = None,
    worker_id: Optional[str] = None,
    min_id: Optional[int] = None,
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Atomically claim a batch of URLs for processing using the PostgreSQL
    claim_product_page_urls() function.
    """
    client = _get_supabase_client()
    if not client:
        print("[!] Supabase client unavailable - cannot claim URLs")
        return [], None
    batch_size = max(0, batch_size)
    if batch_size == 0:
        return [], None

    effective_worker_id = worker_id or str(uuid.uuid4())
    effective_filters = status_filters or ["pending", "retrying"]
    try:
        response = client.rpc(
            "claim_product_page_urls",
            {
                "p_batch_size": batch_size,
                "p_worker_id": effective_worker_id,
                "p_status_filters": effective_filters,
                "p_min_id": min_id,
            },
        ).execute()
        rows = response.data or []
        if not rows:
            return [], effective_worker_id
        return rows, effective_worker_id
    except Exception as exc:
        print(f"[!] Failed to claim URLs batch: {exc}")
        return [], effective_worker_id


def _load_urls_from_database(
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    status_filters: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Load URLs directly from the product_page_urls table.
    Primarily used for dry-run scenarios or single-worker modes.
    """
    client = _get_supabase_client()
    if not client:
        print("[!] Supabase client unavailable - cannot load URLs from database")
        return []

    try:
        query = (
            client.table("product_page_urls")
            .select(
                "id, product_type_id, product_page_url, processing_status, retry_count, claimed_at, claimed_by"
            )
            .order("id")
        )

        effective_filters = status_filters or ["pending", "retrying"]
        if effective_filters:
            query = query.in_("processing_status", effective_filters)

        if offset:
            query = query.range(offset, offset + (limit - 1) if limit else offset + 9999)
        elif limit:
            query = query.limit(limit)

        response = query.execute()
        rows = response.data or []
        return rows
    except Exception as exc:
        print(f"[!] Failed to load URLs from database: {exc}")
        return []


def _update_url_status(
    url_id: int,
    *,
    processing_status: Optional[str] = None,
    success: Optional[bool] = None,
    products_found: Optional[int] = None,
    products_saved: Optional[int] = None,
    error_message: Optional[str] = None,
    retry_count: Optional[int] = None,
    clear_claim: bool = False,
) -> None:
    client = _get_supabase_client()
    if not client or url_id is None:
        return

    payload: Dict[str, Any] = {"updated_at": datetime.now(timezone.utc).isoformat()}
    if processing_status:
        payload["processing_status"] = processing_status
        if processing_status in {"completed", "failed"}:
            payload["processed_at"] = datetime.now(timezone.utc).isoformat()
    if success is not None:
        payload["success"] = success
    if products_found is not None:
        payload["products_found"] = products_found
    if products_saved is not None:
        payload["products_saved"] = products_saved
    if error_message is not None:
        payload["error_message"] = error_message[:500]
    if retry_count is not None:
        payload["retry_count"] = retry_count
    if clear_claim:
        payload["claimed_by"] = None
        payload["claimed_at"] = None

    try:
        client.table("product_page_urls").update(payload).eq("id", url_id).execute()
    except Exception as exc:
        print(f"[!] Failed to update URL status for id={url_id}: {exc}")


def _should_retry(current_retry_count: int, max_retries: int) -> bool:
    if max_retries <= 0:
        return False
    return current_retry_count < max_retries


def _mark_for_retry(
    url_id: int,
    current_retry_count: int,
    error_message: Optional[str],
    max_retries: int,
) -> None:
    next_retry = current_retry_count + 1
    will_retry = next_retry <= max_retries
    new_status = "retrying" if will_retry else "failed"
    _update_url_status(
        url_id,
        processing_status=new_status,
        success=False,
        error_message=error_message,
        retry_count=next_retry,
        clear_claim=True,
    )


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    print("=" * 80)
    print("UNIVERSAL PRODUCT EXTRACTOR")
    print("=" * 80)

    manual_entries = [entry for entry in _load_bulk_urls_from_env() if entry]
    dry_run_size = _get_env_int("DRY_RUN_SAMPLE", 0)
    only_dry_run = _parse_bool_env("DRY_RUN_ONLY", False)
    progress_enabled = _parse_bool_env("PARALLEL_PROGRESS_LOG", True)
    status_filters = _parse_status_filters(os.getenv("DB_URL_STATUS_FILTER"))
    db_limit = _get_env_int("DB_URL_LIMIT", 0)
    db_offset = _get_env_int("DB_URL_OFFSET", 0)

    if manual_entries:
        print(f"[*] Loaded {len(manual_entries)} URL(s) from environment sources")

        with ParallelURLExtractor() as runner:
            def _manual_progress(result: Dict[str, Any], stats_snapshot: Dict[str, Any]) -> None:
                processed = stats_snapshot.get("succeeded", 0) + stats_snapshot.get("failed", 0)
                total = stats_snapshot.get("submitted", 0)
                status = "✓" if result.get("success") else "✗"
                message = (
                    f"{result.get('num_products', 0)} products"
                    if result.get("success")
                    else result.get("error", "error")
                )
                print(f"[{status}] ({processed}/{total}) {result.get('url')} → {message}")

            summary = runner.run_bulk(
                manual_entries,
                progress_callback=_manual_progress if progress_enabled else None,
            )
        _print_bulk_summary(summary)
    else:
        _process_url_batches(
            status_filters=status_filters,
            db_limit=db_limit,
            db_offset=db_offset,
            dry_run_size=dry_run_size,
            only_dry_run=only_dry_run,
            progress_enabled=progress_enabled,
        )

