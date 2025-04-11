import logging
from typing import Dict, Any, List, Optional
from bs4 import BeautifulSoup
import re
import json
import requests
import os
from urllib.parse import urljoin, urlparse, parse_qs
import datetime

logger = logging.getLogger(__name__)


class TravelStackExchangeParser:
    """
    Custom parser for Travel StackExchange to collect user questions and answers
    about immigration from the "customs-and-immigration" tag.
    """
    def __init__(self):
        """
        Initialize the TravelStackExchangeParser.
        """
        self.base_url = "https://travel.stackexchange.com"
        self.tag_url = "https://travel.stackexchange.com/questions/tagged/customs-and-immigration"
        self.output_dir = "./data/travel_stackexchange"
        
        # Create output directories if they don't exist
        os.makedirs(os.path.join(self.output_dir, "questions"), exist_ok=True)
        os.makedirs(os.path.join(self.output_dir, "qa_pairs"), exist_ok=True)
        
    def parse(self, html_content: str, url: str) -> Dict[str, Any]:
        """
        Parse HTML content from Travel StackExchange.
        
        Args:
            html_content: The HTML content to parse
            url: The URL the content was retrieved from
            
        Returns:
            A dictionary containing the parsed data
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        result = {
            'url': url,
            'source': 'Travel StackExchange',
        }
        
        # Extract page title
        title_elem = soup.find('title')
        if title_elem:
            result['title'] = title_elem.get_text(strip=True)
        
        # Determine the type of page and parse accordingly
        if '/questions/tagged/' in url:
            # This is a tag page listing multiple questions
            result.update(self._parse_tag_page(soup, url))
        elif '/questions/' in url and not '/tagged/' in url:
            # This is an individual question page
            result.update(self._parse_question_page(soup, url))
        else:
            # Generic page parsing
            result.update(self._parse_generic_page(soup, url))
            
        return result
    
    def _parse_tag_page(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """
        Parse a tag page listing multiple questions.
        
        Args:
            soup: BeautifulSoup object of the page
            url: The URL the content was retrieved from
            
        Returns:
            A dictionary containing questions list
        """
        result = {
            'content_type': 'tag_page',
            'tag_details': {
                'tag': '',
                'description': '',
                'questions': []
            }
        }
        
        # Extract tag name
        tag_header = soup.find('h1')
        if tag_header:
            result['tag_details']['tag'] = tag_header.get_text(strip=True)
        else:
            # Try to extract from URL
            url_parts = url.split('/')
            if 'tagged' in url_parts:
                tag_index = url_parts.index('tagged') + 1
                if tag_index < len(url_parts):
                    result['tag_details']['tag'] = url_parts[tag_index].replace('-', ' ')
        
        # Extract tag description
        tag_wiki = soup.find('div', class_='tag-wiki')
        if tag_wiki:
            description = tag_wiki.get_text(strip=True)
            result['tag_details']['description'] = description
        
        # Extract questions
        questions = []
        question_elements = soup.find_all('div', class_='question-summary')
        
        for q_elem in question_elements:
            question = {}
            
            # Extract question title and URL
            title_elem = q_elem.find('a', class_='question-hyperlink')
            if title_elem:
                question['title'] = title_elem.get_text(strip=True)
                question['url'] = title_elem['href'] if title_elem.get('href', '').startswith('http') else urljoin(self.base_url, title_elem['href'])
                
                # Extract question ID from URL
                try:
                    question_id = re.search(r'/questions/(\d+)/', question['url'])
                    if question_id:
                        question['id'] = question_id.group(1)
                except Exception:
                    pass
            
            # Extract stats
            stats_container = q_elem.find('div', class_='statscontainer')
            if stats_container:
                # Votes
                votes_elem = stats_container.find('div', class_='votes')
                if votes_elem:
                    votes_count = votes_elem.find('span', class_='vote-count-post')
                    if votes_count:
                        try:
                            question['votes'] = int(votes_count.get_text(strip=True))
                        except ValueError:
                            question['votes'] = votes_count.get_text(strip=True)
                
                # Answers
                answers_elem = stats_container.find('div', class_='status')
                if answers_elem:
                    answers_count = answers_elem.find('strong')
                    if answers_count:
                        try:
                            question['answers'] = int(answers_count.get_text(strip=True))
                        except ValueError:
                            question['answers'] = answers_count.get_text(strip=True)
                            
                    # Check if accepted answer exists
                    if 'accepted-answer' in answers_elem.get('class', []):
                        question['has_accepted_answer'] = True
                
                # Views
                views_elem = stats_container.find('div', class_='views')
                if views_elem:
                    views_text = views_elem.get('title', '') or views_elem.get_text(strip=True)
                    views_match = re.search(r'(\d+)', views_text)
                    if views_match:
                        try:
                            question['views'] = int(views_match.group(1))
                        except ValueError:
                            question['views'] = views_text
            
            # Extract summary/excerpt
            excerpt = q_elem.find('div', class_='excerpt')
            if excerpt:
                question['excerpt'] = excerpt.get_text(strip=True)
            
            # Extract tags
            tags_container = q_elem.find('div', class_='tags')
            if tags_container:
                tags = []
                for tag in tags_container.find_all('a', class_='post-tag'):
                    tags.append(tag.get_text(strip=True))
                question['tags'] = tags
            
            # Extract user and timestamp
            user_container = q_elem.find('div', class_='user-info')
            if user_container:
                user_link = user_container.find('a')
                if user_link:
                    question['user'] = user_link.get_text(strip=True)
                    
                time_elem = user_container.find('span', class_='relativetime')
                if time_elem:
                    question['posted_time'] = time_elem.get('title', '') or time_elem.get_text(strip=True)
            
            if question:  # Only add if we found something
                questions.append(question)
        
        result['tag_details']['questions'] = questions
        result['tag_details']['question_count'] = len(questions)
        
        # Check for pagination
        pagination = {}
        pager = soup.find('div', class_='pager')
        if pager:
            # Current page
            current_page = pager.find('span', class_='current')
            if current_page:
                try:
                    pagination['current_page'] = int(current_page.get_text(strip=True))
                except ValueError:
                    pagination['current_page'] = current_page.get_text(strip=True)
            
            # Total pages or items
            page_count = pager.find_all('a', class_='page-numbers')
            if page_count:
                try:
                    # Get the highest page number
                    highest_page = max([int(p.get_text(strip=True)) for p in page_count 
                                       if p.get_text(strip=True).isdigit()])
                    pagination['total_pages'] = highest_page
                except ValueError:
                    pass
            
            # Next page link
            next_page = pager.find('a', rel='next')
            if next_page and next_page.get('href'):
                pagination['next_page'] = next_page['href'] if next_page['href'].startswith('http') else urljoin(self.base_url, next_page['href'])
                
            # Previous page link
            prev_page = pager.find('a', rel='prev')
            if prev_page and prev_page.get('href'):
                pagination['prev_page'] = prev_page['href'] if prev_page['href'].startswith('http') else urljoin(self.base_url, prev_page['href'])
                
        if pagination:
            result['tag_details']['pagination'] = pagination
            
        # Save to file
        # Create a filename from the tag name
        tag_name = result['tag_details']['tag'].strip().replace(' ', '-').lower()
        
        # Determine page number for filename
        page_num = 1
        if 'pagination' in result['tag_details'] and 'current_page' in result['tag_details']['pagination']:
            page_num = result['tag_details']['pagination']['current_page']
        
        filename = f"{tag_name}_page_{page_num}.json"
        file_path = os.path.join(self.output_dir, "questions", filename)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
            
        logger.info(f"Saved tag page data to {file_path}")
            
        return result
    
    def _parse_question_page(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """
        Parse an individual question page with answers.
        
        Args:
            soup: BeautifulSoup object of the page
            url: The URL the content was retrieved from
            
        Returns:
            A dictionary containing question and answers
        """
        result = {
            'content_type': 'question_page',
            'question_details': {
                'title': '',
                'body': '',
                'tags': [],
                'votes': 0,
                'view_count': 0,
                'answers': []
            }
        }
        
        # Extract question ID from URL
        question_id_match = re.search(r'/questions/(\d+)/', url)
        if question_id_match:
            result['question_details']['id'] = question_id_match.group(1)
        
        # Extract question title
        title_elem = soup.find('h1', id='question-header')
        if title_elem:
            title_text = title_elem.get_text(strip=True)
            result['question_details']['title'] = title_text
        
        # Extract question body
        question_body = soup.find('div', class_='question')
        if question_body:
            # Extract main content
            post_text = question_body.find('div', class_='post-text')
            if post_text:
                # Get HTML content
                result['question_details']['body_html'] = str(post_text)
                # Get plain text
                result['question_details']['body'] = post_text.get_text('\n', strip=True)
            
            # Extract vote count
            vote_count = question_body.find('div', class_='js-vote-count')
            if vote_count:
                try:
                    result['question_details']['votes'] = int(vote_count.get_text(strip=True))
                except ValueError:
                    result['question_details']['votes'] = vote_count.get_text(strip=True)
                    
            # Extract view count
            view_count_div = soup.find('div', class_='views-highlight')
            if view_count_div:
                view_count_text = view_count_div.get_text(strip=True)
                view_count_match = re.search(r'(\d+)', view_count_text.replace(',', ''))
                if view_count_match:
                    try:
                        result['question_details']['view_count'] = int(view_count_match.group(1))
                    except ValueError:
                        result['question_details']['view_count'] = view_count_text
            
            # Extract tags
            tags_container = question_body.find('div', class_='post-taglist')
            if tags_container:
                tags = []
                for tag in tags_container.find_all('a', class_='post-tag'):
                    tags.append(tag.get_text(strip=True))
                result['question_details']['tags'] = tags
            
            # Extract user info
            user_card = question_body.find('div', class_=re.compile('user-info|post-signature'))
            if user_card:
                user_data = {}
                
                # Username
                user_link = user_card.find('a', class_=re.compile('user-link'))
                if user_link:
                    user_data['username'] = user_link.get_text(strip=True)
                    user_data['profile_url'] = user_link['href'] if user_link['href'].startswith('http') else urljoin(self.base_url, user_link['href'])
                
                # Reputation
                reputation = user_card.find('span', class_='reputation-score')
                if reputation:
                    user_data['reputation'] = reputation.get_text(strip=True)
                
                # Timestamp
                timestamp = user_card.find('span', class_='relativetime')
                if timestamp:
                    posted_time = timestamp.get('title', '') or timestamp.get_text(strip=True)
                    user_data['posted_time'] = posted_time
                
                result['question_details']['user'] = user_data
        
        # Extract answers
        answers = []
        answer_elements = soup.find_all('div', class_='answer')
        
        for ans_elem in answer_elements:
            answer = {}
            
            # Extract answer ID
            ans_id = ans_elem.get('data-answerid', '')
            if not ans_id:
                ans_id_match = re.search(r'answer-(\d+)', ans_elem.get('id', ''))
                if ans_id_match:
                    ans_id = ans_id_match.group(1)
            answer['id'] = ans_id
            
            # Check if accepted
            if 'accepted-answer' in ans_elem.get('class', []):
                answer['is_accepted'] = True
            else:
                answer['is_accepted'] = False
            
            # Extract vote count
            vote_count = ans_elem.find('div', class_='js-vote-count')
            if vote_count:
                try:
                    answer['votes'] = int(vote_count.get_text(strip=True))
                except ValueError:
                    answer['votes'] = vote_count.get_text(strip=True)
            
            # Extract answer content
            post_text = ans_elem.find('div', class_='post-text')
            if post_text:
                # Get HTML content
                answer['body_html'] = str(post_text)
                # Get plain text
                answer['body'] = post_text.get_text('\n', strip=True)
            
            # Extract user info
            user_card = ans_elem.find('div', class_=re.compile('user-info|post-signature'))
            if user_card:
                user_data = {}
                
                # Username
                user_link = user_card.find('a', class_=re.compile('user-link'))
                if user_link:
                    user_data['username'] = user_link.get_text(strip=True)
                    user_data['profile_url'] = user_link['href'] if user_link['href'].startswith('http') else urljoin(self.base_url, user_link['href'])
                
                # Reputation
                reputation = user_card.find('span', class_='reputation-score')
                if reputation:
                    user_data['reputation'] = reputation.get_text(strip=True)
                
                # Timestamp
                timestamp = user_card.find('span', class_='relativetime')
                if timestamp:
                    posted_time = timestamp.get('title', '') or timestamp.get_text(strip=True)
                    user_data['posted_time'] = posted_time
                
                answer['user'] = user_data
            
            # Extract comments
            comments_container = ans_elem.find('div', class_='comments')
            if comments_container:
                comments = []
                for comment in comments_container.find_all('div', class_='comment'):
                    comment_data = {}
                    
                    # Comment text
                    comment_text = comment.find('div', class_='comment-text')
                    if comment_text:
                        comment_body = comment_text.find('span', class_='comment-copy')
                        if comment_body:
                            comment_data['text'] = comment_body.get_text(strip=True)
                        
                        # Comment user
                        user_link = comment_text.find('a', class_='comment-user')
                        if user_link:
                            comment_data['user'] = user_link.get_text(strip=True)
                        
                        # Comment timestamp
                        timestamp = comment_text.find('span', class_='relativetime-clean')
                        if timestamp:
                            comment_data['time'] = timestamp.get('title', '') or timestamp.get_text(strip=True)
                    
                    if comment_data:
                        comments.append(comment_data)
                
                if comments:
                    answer['comments'] = comments
            
            answers.append(answer)
        
        result['question_details']['answers'] = answers
        result['question_details']['answer_count'] = len(answers)
        
        # Extract question comments
        question_comments = soup.find('div', id='question')
        if question_comments:
            comments_container = question_comments.find('div', class_='comments')
            if comments_container:
                comments = []
                for comment in comments_container.find_all('div', class_='comment'):
                    comment_data = {}
                    
                    # Comment text
                    comment_text = comment.find('div', class_='comment-text')
                    if comment_text:
                        comment_body = comment_text.find('span', class_='comment-copy')
                        if comment_body:
                            comment_data['text'] = comment_body.get_text(strip=True)
                        
                        # Comment user
                        user_link = comment_text.find('a', class_='comment-user')
                        if user_link:
                            comment_data['user'] = user_link.get_text(strip=True)
                        
                        # Comment timestamp
                        timestamp = comment_text.find('span', class_='relativetime-clean')
                        if timestamp:
                            comment_data['time'] = timestamp.get('title', '') or timestamp.get_text(strip=True)
                    
                    if comment_data:
                        comments.append(comment_data)
                
                if comments:
                    result['question_details']['comments'] = comments
        
        # Save question and its answers as QA pairs for machine learning
        if ('body' in result['question_details'] and result['question_details']['body'] and 
            'answers' in result['question_details'] and result['question_details']['answers']):
            
            qa_pairs = []
            question_text = result['question_details']['body']
            
            for answer in result['question_details']['answers']:
                if 'body' in answer and answer['body']:
                    qa_pair = {
                        'question': question_text,
                        'answer': answer['body'],
                        'is_accepted': answer.get('is_accepted', False),
                        'score': answer.get('votes', 0)
                    }
                    qa_pairs.append(qa_pair)
            
            # Sort by accepted answer first, then by score
            qa_pairs = sorted(qa_pairs, key=lambda x: (-x['is_accepted'], -x['score']))
            
            # Save QA pairs
            if qa_pairs:
                qa_data = {
                    'title': result['question_details'].get('title', ''),
                    'url': url,
                    'tags': result['question_details'].get('tags', []),
                    'qa_pairs': qa_pairs
                }
                
                question_id = result['question_details'].get('id', '')
                if not question_id:
                    question_id = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
                    
                qa_filename = f"qa_pairs_{question_id}.json"
                qa_file_path = os.path.join(self.output_dir, "qa_pairs", qa_filename)
                
                with open(qa_file_path, 'w', encoding='utf-8') as f:
                    json.dump(qa_data, f, indent=2, ensure_ascii=False)
                    
                logger.info(f"Saved QA pairs to {qa_file_path}")
        
        # Save question data
        question_id = result['question_details'].get('id', '')
        if not question_id:
            question_id = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
            
        filename = f"question_{question_id}.json"
        file_path = os.path.join(self.output_dir, "questions", filename)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
            
        logger.info(f"Saved question data to {file_path}")
        
        return result
    
    def _parse_generic_page(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """
        Parse any generic Travel StackExchange page.
        
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
        main_content = soup.find('div', id=re.compile('content|main', re.I)) or \
                      soup.find('div', class_=re.compile('content|main', re.I))
        
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
        
        # Extract links to questions with the customs-and-immigration tag
        question_links = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            if '/questions/' in href and 'customs-and-immigration' in href:
                question_links.append({
                    'url': href if href.startswith('http') else urljoin(self.base_url, href),
                    'text': link.get_text(strip=True)
                })
        
        if question_links:
            result['page_details']['question_links'] = question_links
            
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
    parser = TravelStackExchangeParser()
    return parser.parse(html_content, url) 