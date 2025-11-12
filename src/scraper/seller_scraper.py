"""
Selenium-based scraper for extracting product IDs from AliExpress seller store pages.
"""

import os
import time
import logging
from typing import List, Optional
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)


class SellerStoreScraper:
    """
    Scraper for extracting product IDs from AliExpress seller store pages.
    
    This class handles:
    - Chrome driver setup with headless mode
    - Navigation to seller store pages
    - Country and currency selection (defaults to Germany/EUR)
    - Pagination through all products
    - Product ID extraction
    """

    def __init__(
        self,
        headless: bool = None,
        timeout: int = None,
        country: str = None,
        currency: str = None,
        language: str = None,
    ):
        """
        Initialize the scraper.

        Args:
            headless: Run browser in headless mode (default from env or True)
            timeout: Timeout for waiting for elements in seconds (default from env or 10)
            country: Target country for shipping (default from env or "Germany")
            currency: Target currency for prices (default from env or "EUR")
            language: Target language for interface (default from env or "English")
        """
        self.headless = headless if headless is not None else os.getenv("SELENIUM_HEADLESS", "true").lower() == "true"
        self.timeout = timeout if timeout is not None else int(os.getenv("SELENIUM_TIMEOUT", "10"))
        self.country = country or os.getenv("TARGET_COUNTRY", "Germany")
        self.currency = currency or os.getenv("TARGET_CURRENCY", "EUR")
        self.language = language or os.getenv("TARGET_LANGUAGE", "English")
        self.driver = None

    def _setup_driver(self):
        """Setup Chrome WebDriver with appropriate options."""
        chrome_options = webdriver.ChromeOptions()
        
        # Headless mode configuration - use argument instead of property
        if self.headless:
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--disable-gpu")  # Required for headless on some systems
            chrome_options.add_argument("--no-sandbox")  # Bypass OS security model
            chrome_options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems
            chrome_options.add_argument("--window-size=1920,1080")  # Set window size for headless
        
        # Other options to avoid detection
        chrome_options.add_argument("--log-level=3")
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
        )
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)
        
        # Install ChromeDriver automatically if not present
        service = Service(ChromeDriverManager().install())
        
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # Maximize window if not in headless mode
        if not self.headless:
            self.driver.maximize_window()
        
        logger.info(f"Chrome driver initialized (headless={self.headless})")

    def _set_country_and_currency(self):
        """
        Set the country and currency on the AliExpress page.
        
        This opens the ship-to menu, selects the target country and currency,
        and saves the settings.
        """
        try:
            # Click the ship-to menu button
            logger.info("Opening ship-to menu...")
            choose_menu_button = WebDriverWait(self.driver, self.timeout).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, '[class*="ship-to--menuItem"]'))
            )
            choose_menu_button.click()
            time.sleep(2)
            
            # Wait for the content wrap to be present
            content_wrap = WebDriverWait(self.driver, self.timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '[class*="es--contentWrap"]'))
            )
            
            # Select country
            logger.info(f"Selecting country: {self.country}")
            select_wraps = content_wrap.find_elements(By.CSS_SELECTOR, '[class*="select--wrap"]')
            if len(select_wraps) > 0:
                select_wraps[0].click()
                time.sleep(1)
                
                # Find and click the target country option
                country_options = content_wrap.find_elements(By.CSS_SELECTOR, '[class*="select--item"]')
                for country_option in country_options:
                    if self.country in country_option.text:
                        country_option.click()
                        logger.info(f"✓ Selected {self.country}")
                        break
            
            # Select currency
            logger.info(f"Selecting currency: {self.currency}")
            select_wraps = content_wrap.find_elements(By.CSS_SELECTOR, '[class*="select--wrap"]')
            if len(select_wraps) > 1:
                select_wraps[-1].click()
                time.sleep(1)
                
                # Find and click the currency option
                currency_option = content_wrap.find_element(
                    By.XPATH, 
                    f"//div[contains(@class, 'select--item') and contains(text(), '{self.currency}')]"
                )
                currency_option.click()
                logger.info(f"✓ Selected {self.currency} currency")
                time.sleep(1)
            
            logger.info(f"Selecting Language: {self.language}")
            # Select language from environment variable
            select_wraps = content_wrap.find_elements(By.CSS_SELECTOR, '[class*="select--wrap"]')
            
            select_wraps[-2].click()
            time.sleep(1)
            
            # Find and click the language option
            language_option = content_wrap.find_element(
                By.XPATH, 
                f"//div[contains(@class, 'select--item') and contains(text(), '{self.language}')]"
            )
            language_option.click()
            logger.info(f"✓ Selected {self.language} language")
            time.sleep(1)
            
            # Click save button
            save_button = content_wrap.find_element(By.CSS_SELECTOR, '[class*="es--saveBtn"]')
            save_button.click()
            time.sleep(2)
            logger.info("✓ Country and currency settings saved")
            
        except Exception as e:
            logger.warning(f"Could not set country/currency (continuing anyway): {e}")

    def _accept_cookies(self):
        """Accept cookies if the dialog appears."""
        try:
            accept_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, '//button[contains(text(), "Accept cookies")]'))
            )
            accept_button.click()
            logger.info("✓ Accepted cookies")
            time.sleep(1)
        except Exception:
            logger.debug("No cookie dialog found (or already accepted)")
    
    def _take_screenshot(self, filename: str):
        """Take a screenshot and save it to downloads folder."""
        try:
            screenshot_path = os.path.join("downloads", "screenshots", filename)
            os.makedirs(os.path.dirname(screenshot_path), exist_ok=True)
            self.driver.save_screenshot(screenshot_path)
            logger.info(f"📸 Screenshot saved: {screenshot_path}")
        except Exception as e:
            logger.warning(f"Could not take screenshot: {e}")

    def scrape_seller_products(self, seller_id: str) -> List[str]:
        """
        Scrape all product IDs from a seller's store page.

        Args:
            seller_id: The AliExpress seller/shop ID

        Returns:
            List of product IDs found on the seller's store page

        Raises:
            Exception: If scraping fails
        """
        product_ids = []
        
        try:
            # Setup driver if not already done
            if self.driver is None:
                self._setup_driver()
            
            # Navigate to seller store page
            store_url = f"https://aliexpress.com/store/{seller_id}/pages/all-items.html?shop_sortType=price_desc"
            logger.info(f"Navigating to store page: {store_url}")
            self.driver.get(store_url)
            
            # Wait for page to load
            time.sleep(3)
            logger.info(f"Page loaded: {self.driver.title}")
            
            # Set country and currency
            self._set_country_and_currency()
            
            # Accept cookies if needed
            self._accept_cookies()
            
            
            # Extract products from all pages
            logger.info("Starting product extraction...")
            page_num = 1
            
            while True:
                # Find all product links on the current page
                time.sleep(3)
                WebDriverWait(self.driver, self.timeout).until(
                    EC.presence_of_all_elements_located((By.XPATH, "//a[contains(@ae_object_type, 'product')]"))
                )
                products = self.driver.find_elements(By.XPATH, "//a[contains(@ae_object_type, 'product')]")
                
                
                # Extract product IDs from links
                page_products = []
                for product in products:
                    try:
                        href = product.get_attribute("href")
                        if href:
                            product_id = href.split("/")[-1].split(".html")[0]
                            if product_id and product_id not in product_ids:
                                product_ids.append(product_id)
                                page_products.append(product_id)
                    except Exception as e:
                        logger.warning(f"Error extracting product ID: {e}")
                
                logger.info(f"Page {page_num}: Found {len(page_products)} products (total: {len(product_ids)})")
                
                # Check if there are more pages
                try:
                    time.sleep(3)
                    page_info = self.driver.find_element(By.XPATH, "//div[@currentpage]")
                    current_page = page_info.get_attribute("currentpage")
                    total_page = page_info.get_attribute("totalpage")
                    total_items = page_info.get_attribute("totalcount")
                    
                    # Check if we've reached the last page
                    if current_page == total_page or len(product_ids) >= int(total_items):
                        logger.info(f"✓ Reached last page")
                        logger.info(f"Total items expected: {total_items}, extracted: {len(product_ids)}")
                        break
                    
                    # Click next page button
                    next_buttons = page_info.find_elements(By.TAG_NAME, "div")
                    if next_buttons:
                        next_buttons[-1].click()
                        time.sleep(2)
                        page_num += 1
                    else:
                        break
                        
                except Exception as e:
                    logger.warning(f"Could not find pagination info: {e}")
                    break
            
            logger.info(f"✓ Extraction complete. Total products: {len(product_ids)}")
            # Take screenshot after cookies accepted
            self._take_screenshot(f"seller_{seller_id}.png")
            return product_ids
            
        except Exception as e:
            logger.error(f"Error scraping seller {seller_id}: {e}")
            raise
        
        finally:
            # Keep driver alive for potential reuse
            pass

    def close(self):
        """Close the browser and clean up resources."""
        if self.driver:
            self.driver.quit()
            self.driver = None
            logger.info("Browser closed")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close browser."""
        self.close()


def scrape_seller_store(
    seller_id: str,
    headless: bool = True,
    timeout: int = 10,
    country: str = "Germany",
    currency: str = "EUR",
    language: str = "English",
) -> List[str]:
    """
    Convenience function to scrape a seller's store and return product IDs.

    Args:
        seller_id: The AliExpress seller/shop ID
        headless: Run browser in headless mode
        timeout: Timeout for waiting for elements in seconds
        country: Target country for shipping
        currency: Target currency for prices
        language: Target language for interface

    Returns:
        List of product IDs found on the seller's store page

    Example:
        >>> product_ids = scrape_seller_store("2663214")
        >>> print(f"Found {len(product_ids)} products")
    """
    with SellerStoreScraper(headless=headless, timeout=timeout, country=country, currency=currency, language=language) as scraper:
        return scraper.scrape_seller_products(seller_id)
