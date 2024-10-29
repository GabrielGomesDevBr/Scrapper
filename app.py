# Imports do sistema
import io
import re
import random
import time
import logging
from datetime import datetime
from urllib.parse import quote_plus, urlparse
from concurrent.futures import ThreadPoolExecutor

# Imports de terceiros
import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import json

class AdvancedWebScraper:
    def __init__(self):
        self.user_agents = UserAgent()
        self.session = requests.Session()
        self.found_items = set()
        self.setup_logging()
    
    def setup_logging(self):
        """Configura o sistema de logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('scraping_log.txt')
            ]
        )
    
    def get_random_delay(self, min_delay=1, max_delay=3):
        """Gera um delay aleatório entre requisições"""
        return random.uniform(min_delay, max_delay)
    
    def get_headers(self):
        """Gera headers aleatórios para as requisições"""
        return {
            'User-Agent': self.user_agents.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1'
        }
    
    def extract_data(self, text, patterns):
        """Extrai dados baseados em padrões personalizados"""
        results = {}
        for pattern_name, pattern in patterns.items():
            try:
                matches = set(re.findall(pattern, text, re.IGNORECASE))
                if matches:
                    results[pattern_name] = list(matches)
            except Exception as e:
                logging.error(f"Erro ao processar padrão {pattern_name}: {str(e)}")
                results[pattern_name] = []
        return results
    
    def is_valid_url(self, url):
        """Verifica se a URL é válida"""
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc]) and result.scheme in ['http', 'https']
        except:
            return False
    
    def create_search_url(self, base_url, query, page=1):
        """Gera URLs de busca para diferentes engines"""
        encoded_query = quote_plus(query)
        if "duckduckgo" in base_url:
            return f"https://html.duckduckgo.com/html/?q={encoded_query}"
        elif "google" in base_url:
            return f"https://www.google.com/search?q={encoded_query}&start={(page-1)*10}"
        elif "bing" in base_url:
            return f"https://www.bing.com/search?q={encoded_query}&first={(page-1)*10}"
        return f"{base_url}?q={encoded_query}"

    def scrape_page(self, url, patterns, selector_config):
        """Scraping de uma página individual com configurações personalizadas"""
        try:
            time.sleep(self.get_random_delay())
            response = self.session.get(url, headers=self.get_headers(), timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extrai dados baseado nos seletores configurados
            extracted_data = {
                'url': url,
                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            # Busca por seletores específicos
            for field, selectors in selector_config.items():
                field_data = []
                for selector in selectors:
                    elements = soup.select(selector)
                    if elements:
                        field_data.extend([elem.get_text(strip=True) for elem in elements])
                if field_data:
                    extracted_data[field] = field_data
            
            # Busca por padrões regex no texto completo
            text_content = soup.get_text()
            pattern_matches = self.extract_data(text_content, patterns)
            extracted_data.update(pattern_matches)
            
            return extracted_data
            
        except Exception as e:
            logging.error(f"Erro ao processar {url}: {str(e)}")
            return None

    def scrape_search_results(self, query, patterns, selector_config, search_config):
        """Processo principal de scraping"""
        all_results = []
        
        for page in range(1, search_config['max_pages'] + 1):
            try:
                search_url = self.create_search_url(
                    search_config['search_engine'],
                    query,
                    page
                )
                
                response = self.session.get(search_url, headers=self.get_headers(), timeout=15)
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Extrai URLs dos resultados da busca
                urls = []
                for link in soup.select(search_config['result_selector']):
                    href = link.get('href')
                    if href and self.is_valid_url(href) and not any(excluded in href for excluded in search_config['excluded_domains']):
                        urls.append(href)
                
                # Processa URLs em paralelo
                with ThreadPoolExecutor(max_workers=search_config['max_workers']) as executor:
                    futures = [
                        executor.submit(
                            self.scrape_page,
                            url,
                            patterns,
                            selector_config
                        )
                        for url in urls[:search_config['results_per_page']]
                    ]
                    
                    for future in futures:
                        result = future.result()
                        if result:
                            all_results.append(result)
                
                time.sleep(self.get_random_delay(2, 4))
                
            except Exception as e:
                logging.error(f"Erro na página {page}: {str(e)}")
                continue
                
        return pd.DataFrame(all_results)

def main():
    st.title("Web Scraper Avançado")
    
    st.markdown("""
    ### Configurações de Busca e Extração
    Configure os parâmetros de busca e os padrões de dados que deseja extrair.
    """)
    
    # Configurações de Busca
    search_engine = st.selectbox(
        "Motor de Busca:",
        ["https://html.duckduckgo.com", "https://www.google.com", "https://www.bing.com"]
    )
    
    query = st.text_input("Termos de Busca:", "")
    max_pages = st.slider("Número de Páginas:", 1, 20, 3)
    
    # Configurações de Padrões
    st.subheader("Padrões de Extração")
    
    # Padrões predefinidos
    default_patterns = {
        "emails": r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
        "phones": r'(?:\+55|0)?(?:\s|-)?(?:\d{2})(?:\s|-)?(?:9\s?)?\d{4}(?:\s|-)?\d{4}',
        "websites": r'https?://(?:www\.)?[a-zA-Z0-9-]+\.[a-zA-Z]{2,}(?:/[^\s]*)?'
    }
    
    # Interface para adicionar padrões personalizados
    custom_patterns = {}
    if st.checkbox("Adicionar Padrões Personalizados"):
        pattern_name = st.text_input("Nome do Padrão:")
        pattern_regex = st.text_input("Expressão Regular:")
        if st.button("Adicionar Padrão") and pattern_name and pattern_regex:
            custom_patterns[pattern_name] = pattern_regex
    
    # Combina padrões predefinidos e personalizados
    patterns = {**default_patterns, **custom_patterns}
    
    # Configurações avançadas
    with st.expander("Configurações Avançadas"):
        max_workers = st.slider("Threads Simultâneas:", 1, 10, 3)
        results_per_page = st.slider("Resultados por Página:", 5, 30, 10)
        min_delay = st.slider("Delay Mínimo (s):", 1, 5, 2)
        max_delay = st.slider("Delay Máximo (s):", 3, 10, 5)
    
    if st.button("Iniciar Scraping"):
        if not query:
            st.warning("Por favor, insira os termos de busca.")
            return

        search_config = {
            'search_engine': search_engine,
            'max_pages': max_pages,
            'max_workers': max_workers,
            'results_per_page': results_per_page,
            'result_selector': '.result__url' if 'duckduckgo' in search_engine else 'a',
            'excluded_domains': ['.pdf', '.doc', '.docx', '.xlsx', '.txt']
        }
        
        selector_config = {
            'title': ['h1', '.title', '.post-title'],
            'description': ['meta[name="description"]', '.description', '.summary'],
            'content': ['article', '.content', '.post-content', 'main']
        }
        
        with st.spinner("Realizando scraping..."):
            scraper = AdvancedWebScraper()
            df = scraper.scrape_search_results(
                query,
                patterns,
                selector_config,
                search_config
            )
            
            if df.empty:
                st.warning("Nenhum resultado encontrado. Tente modificar os termos de busca ou padrões.")
                return
            
            st.success("Scraping concluído com sucesso!")
            st.dataframe(df)
            
            # Preparação para download
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Excel
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                df.to_excel(writer, sheet_name='Resultados', index=False)
            
            buffer.seek(0)
            
            # Botão de download Excel
            st.download_button(
                label="Download Excel",
                data=buffer,
                file_name=f"scraping_results_{timestamp}.xlsx",
                mime="application/vnd.ms-excel"
            )
            
            # JSON
            json_str = df.to_json(orient='records', force_ascii=False, indent=2)
            
            # Botão de download JSON
            st.download_button(
                label="Download JSON",
                data=json_str.encode('utf-8'),
                file_name=f"scraping_results_{timestamp}.json",
                mime="application/json"
            )

if __name__ == "__main__":
    main()
