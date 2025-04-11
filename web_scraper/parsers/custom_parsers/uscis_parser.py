import logging
from typing import Dict, Any, List, Optional
from bs4 import BeautifulSoup
import re
import json
import requests
import os
from urllib.parse import urljoin, urlparse
import PyPDF2
from io import BytesIO

logger = logging.getLogger(__name__)


class USCISParser:
    """
    Custom parser for USCIS website data.
    """
    def __init__(self):
        """
        Initialize the USCISParser.
        """
        self.base_url = "https://www.uscis.gov"
        self.output_dir = "./data/uscis"
        
        # Create output directories if they don't exist
        os.makedirs(os.path.join(self.output_dir, "policy_manual"), exist_ok=True)
        os.makedirs(os.path.join(self.output_dir, "forms"), exist_ok=True)
        os.makedirs(os.path.join(self.output_dir, "faqs"), exist_ok=True)
        
    def parse(self, html_content: str, url: str) -> Dict[str, Any]:
        """
        Parse HTML content from USCIS website.
        
        Args:
            html_content: The HTML content to parse
            url: The URL the content was retrieved from
            
        Returns:
            A dictionary containing the parsed data
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        result = {
            'url': url,
            'source': 'USCIS',
        }
        
        # Extract page title
        title_elem = soup.find('title')
        if title_elem:
            result['title'] = title_elem.get_text(strip=True)
        
        # Extract page metadata
        meta_tags = soup.find_all('meta')
        metadata = {}
        for tag in meta_tags:
            if tag.get('name') and tag.get('content'):
                metadata[tag['name']] = tag['content']
            elif tag.get('property') and tag.get('content'):
                metadata[tag['property']] = tag['content']
        result['metadata'] = metadata
        
        # Extract main content based on URL pattern
        if 'forms' in url.lower():
            result.update(self._parse_forms_page(soup, url))
        elif 'policy-manual' in url.lower():
            result.update(self._parse_policy_manual(soup, url))
        elif 'faq' in url.lower() or 'frequently-asked-questions' in url.lower():
            result.update(self._parse_faq_page(soup, url))
        elif 'news' in url.lower() or 'release' in url.lower():
            result.update(self._parse_news_page(soup, url))
        elif 'case-processing-times' in url.lower():
            result.update(self._parse_processing_times(soup, url))
        else:
            # Generic content parsing
            result.update(self._parse_generic_page(soup, url))
            
        return result
        
    def _parse_forms_page(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """
        Parse a USCIS forms page.
        
        Args:
            soup: BeautifulSoup object of the page
            url: The URL the content was retrieved from
            
        Returns:
            A dictionary containing form-specific data
        """
        result = {
            'content_type': 'form',
            'form_details': {}
        }
        
        # Try to extract form number and name
        form_title = soup.find('h1')
        if form_title:
            form_text = form_title.get_text(strip=True)
            # Try to match form number pattern (e.g., I-485, N-400)
            form_match = re.search(r'([A-Z]-\d+)', form_text)
            if form_match:
                result['form_details']['form_number'] = form_match.group(1)
                result['form_details']['form_name'] = form_text
                
        # Extract form download links
        pdf_links = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.endswith('.pdf'):
                link_text = link.get_text(strip=True)
                pdf_info = {
                    'url': href if href.startswith('http') else f"{self.base_url}{href}",
                    'text': link_text
                }
                pdf_links.append(pdf_info)
                
                # Download the PDF and extract form fields
                if 'form' in link_text.lower() and not 'instruction' in link_text.lower():
                    try:
                        pdf_info['fields'] = self._extract_pdf_form_fields(pdf_info['url'])
                    except Exception as e:
                        logger.error(f"Error extracting PDF form fields: {e}")
                        
        result['form_details']['pdf_links'] = pdf_links
        
        # Extract form filing fee information
        fee_section = soup.find(lambda tag: tag.name in ['div', 'h2', 'h3', 'h4'] and 
                               re.search('Filing Fee', tag.get_text(), re.I))
                      
        if fee_section:
            # Get the next paragraph or list
            fee_info = []
            next_elem = fee_section.find_next(['p', 'ul', 'ol', 'div'])
            while next_elem and next_elem.name in ['p', 'ul', 'ol', 'li', 'div', 'span']:
                fee_info.append(next_elem.get_text(strip=True))
                next_elem = next_elem.find_next(['p', 'ul', 'ol', 'div'])
                
            result['form_details']['filing_fee'] = ' '.join(fee_info)
            
        # Extract form filing eligibility
        eligibility_section = soup.find(lambda tag: tag.name in ['div', 'h2', 'h3', 'h4'] and 
                                       re.search('Eligibility', tag.get_text(), re.I))
                             
        if eligibility_section:
            # Get the next paragraph or list
            eligibility_info = []
            next_elem = eligibility_section.find_next(['p', 'ul', 'ol', 'div'])
            while next_elem and next_elem.name in ['p', 'ul', 'ol', 'li', 'div', 'span']:
                eligibility_info.append(next_elem.get_text(strip=True))
                next_elem = next_elem.find_next(['p', 'ul', 'ol', 'div'])
                
            result['form_details']['eligibility'] = ' '.join(eligibility_info)
            
        # Extract required documentation
        required_docs_section = soup.find(lambda tag: tag.name in ['div', 'h2', 'h3', 'h4'] and 
                                         re.search('Required Documentation|What to Submit', tag.get_text(), re.I))
                               
        if required_docs_section:
            # Get the next paragraph or list
            required_docs = []
            next_elem = required_docs_section.find_next(['p', 'ul', 'ol', 'div'])
            while next_elem and next_elem.name in ['p', 'ul', 'ol', 'li', 'div', 'span']:
                required_docs.append(next_elem.get_text(strip=True))
                next_elem = next_elem.find_next(['p', 'ul', 'ol', 'div'])
                
            result['form_details']['required_documentation'] = ' '.join(required_docs)
            
        # Extract processing times if available
        processing_section = soup.find(lambda tag: tag.name in ['div', 'h2', 'h3', 'h4'] and 
                                      re.search('Processing Time', tag.get_text(), re.I))
                            
        if processing_section:
            # Get the next paragraph or list
            processing_info = []
            next_elem = processing_section.find_next(['p', 'ul', 'ol', 'div'])
            while next_elem and next_elem.name in ['p', 'ul', 'ol', 'li', 'div', 'span']:
                processing_info.append(next_elem.get_text(strip=True))
                next_elem = next_elem.find_next(['p', 'ul', 'ol', 'div'])
                
            result['form_details']['processing_time'] = ' '.join(processing_info)
            
        # Extract where to file information
        filing_location = soup.find(lambda tag: tag.name in ['div', 'h2', 'h3', 'h4'] and 
                                   re.search('Where to File|Filing Location', tag.get_text(), re.I))
                         
        if filing_location:
            # Get the next paragraph or list
            location_info = []
            next_elem = filing_location.find_next(['p', 'ul', 'ol', 'div'])
            while next_elem and next_elem.name in ['p', 'ul', 'ol', 'li', 'div', 'span']:
                location_info.append(next_elem.get_text(strip=True))
                next_elem = next_elem.find_next(['p', 'ul', 'ol', 'div'])
                
            result['form_details']['filing_location'] = ' '.join(location_info)
            
        return result
    
    def _extract_pdf_form_fields(self, pdf_url: str) -> List[Dict[str, str]]:
        """
        Download a PDF form and extract its field information.
        
        Args:
            pdf_url: URL to the PDF form
            
        Returns:
            List of dictionaries containing field name, type, and description
        """
        try:
            # Download the PDF
            response = requests.get(pdf_url, stream=True)
            response.raise_for_status()
            
            # Parse the PDF
            pdf_reader = PyPDF2.PdfReader(BytesIO(response.content))
            
            # Extract form fields if it's a fillable form
            fields = []
            
            if pdf_reader.get_fields():
                for field_name, field_data in pdf_reader.get_fields().items():
                    fields.append({
                        'name': field_name,
                        'type': field_data.get('/FT', 'Unknown'),
                        'value': field_data.get('/V', '')
                    })
            else:
                # If no form fields (not a fillable PDF), extract text and look for form fields
                for page_num in range(len(pdf_reader.pages)):
                    page = pdf_reader.pages[page_num]
                    text = page.extract_text()
                    
                    # Look for patterns that might indicate form fields
                    # Example: "1. Last Name: ___________"
                    field_patterns = [
                        r'(\d+\.?\s*[A-Za-z\s]+):\s*[_]+',
                        r'([A-Za-z\s]+):\s*[_]+',
                        r'(\d+\.?\s*[A-Za-z\s]+)\s*\[\s*\]'
                    ]
                    
                    for pattern in field_patterns:
                        for match in re.finditer(pattern, text):
                            field_name = match.group(1).strip()
                            fields.append({
                                'name': field_name,
                                'page': page_num + 1,
                                'type': 'Text' if '_' in match.group(0) else 'Checkbox'
                            })
            
            # Save PDF file to disk for reference
            filename = os.path.basename(urlparse(pdf_url).path)
            output_path = os.path.join(self.output_dir, "forms", filename)
            
            with open(output_path, 'wb') as f:
                f.write(response.content)
                
            logger.info(f"Saved PDF to {output_path}")
            
            return fields
            
        except Exception as e:
            logger.error(f"Error processing PDF {pdf_url}: {str(e)}")
            return []
        
    def _parse_policy_manual(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """
        Parse a USCIS policy manual page.
        
        Args:
            soup: BeautifulSoup object of the page
            url: The URL the content was retrieved from
            
        Returns:
            A dictionary containing policy manual data
        """
        result = {
            'content_type': 'policy_manual',
            'policy_details': {}
        }
        
        # Extract volume and chapter information
        title_elem = soup.find('h1')
        if title_elem:
            title_text = title_elem.get_text(strip=True)
            result['policy_details']['title'] = title_text
            
            # Try to extract volume and chapter info
            volume_match = re.search(r'Volume\s+(\d+)', title_text, re.I)
            if volume_match:
                result['policy_details']['volume'] = volume_match.group(1)
                
            chapter_match = re.search(r'Chapter\s+(\d+)', title_text, re.I)
            if chapter_match:
                result['policy_details']['chapter'] = chapter_match.group(1)
                
            part_match = re.search(r'Part\s+([A-Z])', title_text, re.I)
            if part_match:
                result['policy_details']['part'] = part_match.group(1)
            
        # Extract policy content by sections
        content_div = soup.find('div', class_=re.compile('content|main-content|text-content|body-content'))
        if content_div:
            sections = []
            
            # Find all headers which likely represent sections
            headers = content_div.find_all(['h2', 'h3', 'h4'])
            
            for i, header in enumerate(headers):
                section = {
                    'section_title': header.get_text(strip=True),
                    'section_number': i + 1,
                    'content': []
                }
                
                # Get content until the next header or end
                curr_elem = header.next_sibling
                while curr_elem and (i == len(headers) - 1 or curr_elem != headers[i+1]):
                    if hasattr(curr_elem, 'name') and curr_elem.name in ['p', 'ul', 'ol', 'div', 'table']:
                        section['content'].append(curr_elem.get_text(strip=True))
                    curr_elem = curr_elem.next_sibling
                    
                section['text'] = '\n'.join(section['content'])
                sections.append(section)
                
            result['policy_details']['sections'] = sections
            
            # Extract the full text as well
            all_paragraphs = content_div.find_all(['p', 'li'])
            full_text = '\n'.join(p.get_text(strip=True) for p in all_paragraphs)
            result['policy_details']['full_text'] = full_text
            
        # Extract update information
        update_info = soup.find(string=re.compile('Updated|Last Reviewed|Published', re.I))
        if update_info:
            parent = update_info.parent
            if parent:
                result['policy_details']['update_info'] = parent.get_text(strip=True)
                
        # Extract table of contents or navigation
        toc = soup.find('nav') or soup.find('div', class_=re.compile('toc|table-of-contents|nav'))
        if toc:
            toc_links = []
            for link in toc.find_all('a', href=True):
                toc_links.append({
                    'text': link.get_text(strip=True),
                    'url': link['href'] if link['href'].startswith('http') else f"{self.base_url}{link['href']}"
                })
            result['policy_details']['table_of_contents'] = toc_links
            
        # Save policy manual as JSON
        if 'volume' in result['policy_details'] and 'chapter' in result['policy_details']:
            volume = result['policy_details']['volume']
            chapter = result['policy_details']['chapter']
            
            # Create volume subdirectory if it doesn't exist
            volume_dir = os.path.join(self.output_dir, "policy_manual", f"volume_{volume}")
            os.makedirs(volume_dir, exist_ok=True)
            
            # Save chapter as JSON file
            filename = f"chapter_{chapter}.json"
            file_path = os.path.join(volume_dir, filename)
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
                
            logger.info(f"Saved policy manual to {file_path}")
            
        return result
    
    def _parse_faq_page(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """
        Parse a USCIS FAQ page to extract question-answer pairs.
        
        Args:
            soup: BeautifulSoup object of the page
            url: The URL the content was retrieved from
            
        Returns:
            A dictionary containing FAQ data
        """
        result = {
            'content_type': 'faq',
            'faq_details': {
                'title': '',
                'qa_pairs': []
            }
        }
        
        # Extract title
        title_elem = soup.find('h1')
        if title_elem:
            result['faq_details']['title'] = title_elem.get_text(strip=True)
            
        # Try to find FAQ sections - common patterns:
        # 1. Accordion-style FAQ with question as header and answer as content
        accordion = soup.find_all('div', class_=re.compile('accordion|faq|collapse'))
        if accordion:
            for section in accordion:
                question_elem = section.find(['h2', 'h3', 'h4', 'button', 'a'])
                if question_elem:
                    question = question_elem.get_text(strip=True)
                    # Find the corresponding answer
                    answer_elem = section.find(['div', 'p'], class_=re.compile('content|body|answer|panel'))
                    answer = answer_elem.get_text(strip=True) if answer_elem else ""
                    
                    if question and answer:
                        result['faq_details']['qa_pairs'].append({
                            'question': question,
                            'answer': answer
                        })
                        
        # 2. Q and A with distinct classes
        q_elems = soup.find_all(['div', 'p', 'h3', 'h4'], class_=re.compile('question|faq-question|q'))
        for q_elem in q_elems:
            question = q_elem.get_text(strip=True)
            # Look for the next element with answer class
            a_elem = q_elem.find_next(['div', 'p'], class_=re.compile('answer|faq-answer|a'))
            answer = a_elem.get_text(strip=True) if a_elem else ""
            
            if question and answer:
                result['faq_details']['qa_pairs'].append({
                    'question': question,
                    'answer': answer
                })
                
        # 3. Simple pattern of h3/h4 followed by paragraphs
        if not result['faq_details']['qa_pairs']:
            content_div = soup.find('div', class_=re.compile('content|main-content|text-content|body-content'))
            if content_div:
                headers = content_div.find_all(['h2', 'h3', 'h4'])
                
                for header in headers:
                    question = header.get_text(strip=True)
                    
                    # If the text looks like a question
                    if question.endswith('?') or re.search(r'^(what|how|when|where|who|why|is|are|can|do|does)', 
                                                         question, re.I):
                        # Get all paragraphs until the next header
                        answer_parts = []
                        next_elem = header.next_sibling
                        
                        while next_elem and next_elem.name not in ['h2', 'h3', 'h4']:
                            if hasattr(next_elem, 'name') and next_elem.name in ['p', 'ul', 'ol', 'div']:
                                answer_parts.append(next_elem.get_text(strip=True))
                            next_elem = next_elem.next_sibling
                            
                        answer = '\n'.join(answer_parts)
                        
                        if question and answer:
                            result['faq_details']['qa_pairs'].append({
                                'question': question,
                                'answer': answer
                            })
                            
        # Save FAQ data
        if result['faq_details']['qa_pairs']:
            # Create a filename from the page title
            title = result['faq_details']['title']
            safe_title = re.sub(r'[^\w\s]', '', title).strip().replace(' ', '_').lower()
            
            if not safe_title:
                safe_title = 'faq_' + str(abs(hash(url)) % 10000)
                
            filename = f"{safe_title}.json"
            file_path = os.path.join(self.output_dir, "faqs", filename)
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
                
            logger.info(f"Saved FAQ data to {file_path}")
                
        return result
        
    def _parse_news_page(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """
        Parse a USCIS news or press release page.
        
        Args:
            soup: BeautifulSoup object of the page
            url: The URL the content was retrieved from
            
        Returns:
            A dictionary containing news data
        """
        result = {
            'content_type': 'news',
            'news_details': {}
        }
        
        # Extract news title
        news_title = soup.find('h1')
        if news_title:
            result['news_details']['title'] = news_title.get_text(strip=True)
            
        # Extract publication date
        date_elem = soup.find(string=re.compile('Released on:|Date:', re.I))
        if date_elem:
            parent = date_elem.parent
            if parent:
                result['news_details']['date'] = parent.get_text(strip=True)
        else:
            # Alternative date patterns
            date_elem = soup.find(class_=re.compile('date|published|meta'))
            if date_elem:
                result['news_details']['date'] = date_elem.get_text(strip=True)
                
        # Extract news content
        content_div = soup.find('div', class_=re.compile('content|main-content|text-content|body-content'))
        if content_div:
            paragraphs = content_div.find_all('p')
            content_text = '\n'.join(p.get_text(strip=True) for p in paragraphs)
            result['news_details']['content'] = content_text
            
        # Extract related links
        related_section = soup.find(string=re.compile('Related|See also|For more information', re.I))
        if related_section:
            parent = related_section.parent
            if parent:
                related_links = []
                for link in parent.find_all_next('a', href=True, limit=5):
                    related_links.append({
                        'text': link.get_text(strip=True),
                        'url': link['href'] if link['href'].startswith('http') else f"{self.base_url}{link['href']}"
                    })
                result['news_details']['related_links'] = related_links
                
        return result
        
    def _parse_processing_times(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """
        Parse a USCIS case processing times page.
        
        Args:
            soup: BeautifulSoup object of the page
            url: The URL the content was retrieved from
            
        Returns:
            A dictionary containing processing time data
        """
        result = {
            'content_type': 'processing_times',
            'processing_details': {}
        }
        
        # Extract processing time data
        # This is often in tables or specialized components
        tables = soup.find_all('table')
        processing_data = []
        
        for table in tables:
            # Extract headers
            headers = []
            header_row = table.find('tr')
            if header_row:
                for th in header_row.find_all(['th', 'td']):
                    headers.append(th.get_text(strip=True))
                    
            # Extract data rows
            rows = []
            for tr in table.find_all('tr')[1:]:  # Skip header row
                row_data = {}
                cells = tr.find_all(['td', 'th'])
                for i, cell in enumerate(cells):
                    if i < len(headers):
                        row_data[headers[i]] = cell.get_text(strip=True)
                    else:
                        row_data[f"Column_{i+1}"] = cell.get_text(strip=True)
                if row_data:
                    rows.append(row_data)
                    
            if rows:
                processing_data.append({
                    'headers': headers,
                    'rows': rows
                })
                
        result['processing_details']['tables'] = processing_data
        
        # Look for JavaScript data
        # USCIS often stores processing time data in JavaScript variables
        scripts = soup.find_all('script')
        for script in scripts:
            script_text = script.string
            if script_text and ('processingTime' in script_text or 'processing_time' in script_text):
                # Try to extract JSON data
                json_match = re.search(r'var\s+(\w+)\s*=\s*({.*?});', script_text, re.DOTALL)
                if json_match:
                    try:
                        json_data = json.loads(json_match.group(2))
                        result['processing_details']['json_data'] = json_data
                    except json.JSONDecodeError:
                        # If JSON parsing fails, include the raw match
                        result['processing_details']['raw_data'] = json_match.group(2)
                        
        return result
        
    def _parse_generic_page(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """
        Parse any generic USCIS page.
        
        Args:
            soup: BeautifulSoup object of the page
            url: The URL the content was retrieved from
            
        Returns:
            A dictionary containing generic page data
        """
        result = {
            'content_type': 'generic',
            'page_details': {}
        }
        
        # Extract main content
        main_content = soup.find('main') or soup.find('div', id=re.compile('content|main', re.I))
        if main_content:
            # Extract headings
            headings = []
            for h in main_content.find_all(['h1', 'h2', 'h3', 'h4']):
                headings.append({
                    'level': int(h.name[1]),
                    'text': h.get_text(strip=True)
                })
            result['page_details']['headings'] = headings
            
            # Extract paragraphs
            paragraphs = main_content.find_all('p')
            result['page_details']['content'] = '\n'.join(p.get_text(strip=True) for p in paragraphs)
            
            # Extract lists
            lists = []
            for list_elem in main_content.find_all(['ul', 'ol']):
                list_items = []
                for li in list_elem.find_all('li'):
                    list_items.append(li.get_text(strip=True))
                lists.append({
                    'type': list_elem.name,
                    'items': list_items
                })
            result['page_details']['lists'] = lists
            
        # Extract links
        links = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            if href.startswith('/') or href.startswith(self.base_url):
                links.append({
                    'text': a.get_text(strip=True),
                    'url': href if href.startswith('http') else f"{self.base_url}{href}"
                })
        result['page_details']['internal_links'] = links
        
        return result


# Function version for compatibility with the CLI
def parse(html_content: str, url: str) -> Dict[str, Any]:
    """
    Parse function for CLI compatibility.
    
    Args:
        html_content: The HTML content to parse
        url: The URL the content was retrieved from
        
    Returns:
        A dictionary containing the parsed data
    """
    parser = USCISParser()
    return parser.parse(html_content, url) 