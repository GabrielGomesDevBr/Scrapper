# Imports do sistema
import io
import re
import random
import time
import logging
from datetime import datetime
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional

# Imports de terceiros
import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

class DirectWebScraper:
    def __init__(self):
        self.user_agents = UserAgent()
        self.session = requests.Session()
        self.setup_logging()
        
        # Padrões predefinidos de extração
        self.default_patterns = {
            'email': r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
            'telefone_br': r'(?:\+55\s?)?(?:\(?\d{2}\)?[\s-]?)?\d{4,5}[-\s]?\d{4}',
            'celular_br': r'(?:\+55\s?)?(?:\(?\d{2}\)?[\s-]?)?9\d{4}[-\s]?\d{4}',
            'whatsapp': r'(?:whatsapp|whats|wpp|wap)(?:\s*:)?\s*(?:\+55\s?)?(?:\(?\d{2}\)?[\s-]?)?9?\d{4}[-\s]?\d{4}',
            'cnpj': r'\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2}',
            'cep': r'\d{5}-?\d{3}',
            'instagram': r'(?:instagram\.com|instagr\.am|ig)/([A-Za-z0-9_](?:(?:[A-Za-z0-9_]|(?:\.(?!\.))){0,28}(?:[A-Za-z0-9_]))?)',
            'facebook': r'(?:facebook\.com|fb\.com)/(?:(?:\w)*#!\/)?(?:pages\/)?(?:[?\w\-]*\/)*?([A-Za-z0-9_](?:(?:[A-Za-z0-9_]|(?:\.(?!\.))){0,28}(?:[A-Za-z0-9_]))?)',
            'linkedin': r'linkedin\.com/(?:in|company)/([A-Za-z0-9_-]+)',
        }
        
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
            'Upgrade-Insecure-Requests': '1'
        }
    
    def is_valid_url(self, url: str) -> bool:
        """Verifica se a URL é válida"""
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc]) and result.scheme in ['http', 'https']
        except:
            return False
    
    def extract_data_from_text(self, text: str, patterns: Dict[str, str]) -> Dict[str, List[str]]:
        """Extrai dados do texto baseado nos padrões fornecidos"""
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
    
    def normalize_url(self, url: str) -> str:
        """Normaliza a URL fornecida"""
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        return url
    
    def get_internal_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Extrai links internos da página"""
        internal_links = []
        domain = urlparse(base_url).netloc
        
        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.startswith('/'):
                href = base_url + href
            elif href.startswith(('http://', 'https://')):
                if domain in href:
                    internal_links.append(href)
            elif not href.startswith(('#', 'mailto:', 'tel:', 'javascript:')):
                href = base_url + '/' + href
                internal_links.append(href)
                
        return list(set(internal_links))
    
    def get_page_content(self, url: str) -> Optional[BeautifulSoup]:
        """Obtém o conteúdo de uma página"""
        try:
            time.sleep(self.get_random_delay())
            response = self.session.get(url, headers=self.get_headers(), timeout=15)
            response.raise_for_status()
            return BeautifulSoup(response.text, 'html.parser')
        except Exception as e:
            logging.error(f"Erro ao acessar {url}: {str(e)}")
            return None
        
    def scrape_url(self, url: str, patterns: Dict[str, str], max_internal_pages: int = 0) -> pd.DataFrame:
        """Realiza o scraping da URL e opcionalmente de suas páginas internas"""
        all_results = []
        processed_urls = set()
        urls_to_process = [self.normalize_url(url)]
        
        while urls_to_process and len(processed_urls) <= max_internal_pages:
            current_url = urls_to_process.pop(0)
            
            if current_url in processed_urls:
                continue
                
            processed_urls.add(current_url)
            logging.info(f"Processando: {current_url}")
            
            soup = self.get_page_content(current_url)
            if not soup:
                continue
            
            # Extrai dados da página atual
            text_content = soup.get_text()
            extracted_data = self.extract_data_from_text(text_content, patterns)
            
            if any(extracted_data.values()):
                result = {
                    'url': current_url,
                    'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                result.update(extracted_data)
                all_results.append(result)
            
            # Adiciona links internos se necessário
            if len(processed_urls) < max_internal_pages:
                internal_links = self.get_internal_links(soup, url)
                urls_to_process.extend([link for link in internal_links if link not in processed_urls])
        
        # Cria DataFrame com os resultados
        df = pd.DataFrame(all_results)
        if not df.empty:
            # Explode listas em colunas separadas
            for col in df.columns:
                if df[col].apply(type).eq(list).any():
                    df = df.explode(col)
        
        return df

def main():
    st.set_page_config(page_title="Web Scraper Direto", layout="wide")
    
    st.title("Web Scraper Direto")
    st.markdown("""
    ### Extraia dados específicos de qualquer website
    Insira a URL e selecione os tipos de dados que deseja extrair.
    """)
    
    # Input da URL
    url = st.text_input("URL do site:", placeholder="Exemplo: www.exemplo.com.br")
    
    # Configurações de scraping
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Dados para Extrair")
        scraper = DirectWebScraper()
        
        # Seleção de padrões predefinidos
        selected_patterns = {}
        for pattern_name in scraper.default_patterns.keys():
            if st.checkbox(pattern_name.replace('_', ' ').title()):
                selected_patterns[pattern_name] = scraper.default_patterns[pattern_name]
    
    with col2:
        st.subheader("Configurações Avançadas")
        max_internal_pages = st.slider(
            "Número máximo de páginas internas:",
            0, 20, 0,
            help="0 = apenas a página principal, >0 = inclui páginas internas"
        )
        
        # Padrão personalizado
        if st.checkbox("Adicionar Padrão Personalizado"):
            pattern_name = st.text_input("Nome do padrão:")
            pattern_regex = st.text_input("Expressão regular:")
            if pattern_name and pattern_regex:
                selected_patterns[pattern_name] = pattern_regex
    
    if st.button("Iniciar Scraping"):
        if not url:
            st.warning("Por favor, insira uma URL.")
            return
            
        if not selected_patterns:
            st.warning("Selecione pelo menos um tipo de dado para extrair.")
            return
        
        with st.spinner("Realizando scraping..."):
            try:
                df = scraper.scrape_url(url, selected_patterns, max_internal_pages)
                
                if df.empty:
                    st.warning("Nenhum dado encontrado com os padrões selecionados.")
                    return
                
                st.success("Scraping concluído com sucesso!")
                
                # Exibe resultados
                st.subheader("Resultados Encontrados")
                st.dataframe(df)
                
                # Prepara downloads
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                # Excel
                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                    df.to_excel(writer, sheet_name='Resultados', index=False)
                buffer.seek(0)
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.download_button(
                        label="Download Excel",
                        data=buffer,
                        file_name=f"scraping_results_{timestamp}.xlsx",
                        mime="application/vnd.ms-excel"
                    )
                
                with col2:
                    # JSON
                    json_str = df.to_json(orient='records', force_ascii=False, indent=2)
                    st.download_button(
                        label="Download JSON",
                        data=json_str.encode('utf-8'),
                        file_name=f"scraping_results_{timestamp}.json",
                        mime="application/json"
                    )
                    
            except Exception as e:
                st.error(f"Erro durante o scraping: {str(e)}")
                logging.error(f"Erro: {str(e)}")

if __name__ == "__main__":
    main()    
