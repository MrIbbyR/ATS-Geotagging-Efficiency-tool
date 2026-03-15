# -*- coding: utf-8 -*-
"""
IMPROVED NER-based location extraction for resumes
Key improvements:
1. Better handling of sparse/short text
2. Enhanced Indian location recognition
3. Multi-stage fallback with confidence scoring
4. Better header pattern matching
"""

import re
from typing import Optional, List, Dict, Tuple
from dataclasses import dataclass, field

# Try importing spaCy
try:
    import spacy
    SPACY_AVAILABLE = True
except ImportError:
    SPACY_AVAILABLE = False
    print("⚠️  spaCy not installed. Run: pip install spacy --break-system-packages")
    print("⚠️  Then download model: python -m spacy download en_core_web_sm")

# Gazetteer data
import geonamescache

@dataclass
class LocationResult:
    """Result from NER location extraction"""
    city: Optional[str] = None
    region: Optional[str] = None  # State/Province
    country: Optional[str] = None
    confidence: float = 0.0
    method: str = "ner"
    raw_entities: List[str] = field(default_factory=list)
    fallback_used: bool = False
    debug_info: Dict = field(default_factory=dict)


class ImprovedLocationNER:
    """Enhanced location extraction with better fallback logic"""
    
    # Indian states and union territories for better matching
    INDIAN_STATES = {
        "andhra pradesh", "arunachal pradesh", "assam", "bihar", "chhattisgarh",
        "goa", "gujarat", "haryana", "himachal pradesh", "jharkhand", "karnataka",
        "kerala", "madhya pradesh", "maharashtra", "manipur", "meghalaya", "mizoram",
        "nagaland", "odisha", "punjab", "rajasthan", "sikkim", "tamil nadu",
        "telangana", "tripura", "uttar pradesh", "uttarakhand", "west bengal",
        # Union territories
        "andaman and nicobar", "chandigarh", "dadra and nagar haveli",
        "daman and diu", "delhi", "jammu and kashmir", "ladakh", "lakshadweep",
        "puducherry"
    }
    
    # Common Indian cities
    INDIAN_CITIES = {
        "mumbai", "delhi", "bangalore", "bengaluru", "hyderabad", "ahmedabad",
        "chennai", "kolkata", "surat", "pune", "jaipur", "lucknow", "kanpur",
        "nagpur", "indore", "thane", "bhopal", "visakhapatnam", "pimpri-chinchwad",
        "patna", "vadodara", "ghaziabad", "ludhiana", "agra", "nashik", "faridabad",
        "meerut", "rajkot", "kalyan-dombivali", "vasai-virar", "varanasi", "srinagar",
        "aurangabad", "dhanbad", "amritsar", "navi mumbai", "allahabad", "prayagraj",
        "ranchi", "howrah", "coimbatore", "jabalpur", "gwalior", "vijayawada",
        "jodhpur", "madurai", "raipur", "kota", "guwahati", "chandigarh", "solapur",
        "hubli-dharwad", "mysore", "mysuru", "tiruchirappalli", "tiruppur", "bareilly",
        "moradabad", "gurgaon", "gurugram", "noida", "greater noida", "kochi"
    }
    
    def __init__(self):
        """Initialize improved NER model"""
        if not SPACY_AVAILABLE:
            raise ImportError("spaCy is not available")
        
        try:
            # Load spaCy model
            self.nlp = spacy.load("en_core_web_sm")
            print("✅ Loaded spaCy model: en_core_web_sm")
        except OSError:
            print("⚠️  Model not found. Downloading en_core_web_sm...")
            import subprocess
            subprocess.run(["python", "-m", "spacy", "download", "en_core_web_sm"])
            self.nlp = spacy.load("en_core_web_sm")
        
        # Build gazetteer
        self._gc = geonamescache.GeonamesCache()
        
        # Build country index with more aliases
        self.countries = {}
        self.country_aliases = {
            "uk": "United Kingdom",
            "gb": "United Kingdom", 
            "great britain": "United Kingdom",
            "usa": "United States",
            "us": "United States",
            "u.s.": "United States",
            "u.s.a.": "United States",
            "uae": "United Arab Emirates",
            "u.a.e.": "United Arab Emirates",
            "india": "India",
            "bharat": "India",
        }
        
        for c in self._gc.get_countries().values():
            name = c["name"].lower()
            self.countries[name] = c
            # Add ISO codes as aliases
            self.countries[c["iso"].lower()] = c
            self.countries[c["iso3"].lower()] = c
        
        # Build city index with country codes and population
        self.cities = {}
        for city in self._gc.get_cities().values():
            name = city["name"].lower()
            if name not in self.cities:
                self.cities[name] = []
            self.cities[name].append({
                "name": city["name"],
                "country": city["countrycode"],
                "population": int(city.get("population", 0)),
                "admin1": city.get("admin1code", "")
            })
        
        print(f"✅ Gazetteer loaded: {len(self.countries)} countries, {len(self.cities)} cities")
        print(f"✅ Indian location awareness: {len(self.INDIAN_STATES)} states, {len(self.INDIAN_CITIES)} major cities")
    
    def _normalize_text(self, text: str) -> str:
        """Clean and normalize text"""
        # Remove URLs
        text = re.sub(r'https?://\S+', ' ', text)
        # Remove emails
        text = re.sub(r'\S+@\S+', ' ', text)
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        return text.strip()
    
    def _is_country(self, text: str) -> Optional[Dict]:
        """Check if text is a country with fuzzy matching"""
        text_lower = text.lower().strip()
        
        # Direct match
        if text_lower in self.countries:
            return self.countries[text_lower]
        
        # Check alias
        if text_lower in self.country_aliases:
            canonical = self.country_aliases[text_lower].lower()
            return self.countries.get(canonical)
        
        # Fuzzy match for common variations
        text_clean = re.sub(r'[^\w\s]', '', text_lower)
        for country_name, country_data in self.countries.items():
            country_clean = re.sub(r'[^\w\s]', '', country_name)
            if country_clean == text_clean:
                return country_data
        
        return None
    
    def _is_indian_state(self, text: str) -> bool:
        """Check if text is an Indian state"""
        text_lower = text.lower().strip()
        return text_lower in self.INDIAN_STATES
    
    def _is_indian_city(self, text: str) -> bool:
        """Check if text is a major Indian city"""
        text_lower = text.lower().strip()
        return text_lower in self.INDIAN_CITIES
    
    def _is_city(self, text: str) -> Optional[List[Dict]]:
        """Check if text is a city with enhanced Indian city detection"""
        text_lower = text.lower().strip()
        
        # Check gazetteer
        cities = self.cities.get(text_lower)
        if cities:
            return cities
        
        # For Indian cities, be more flexible
        if self._is_indian_city(text_lower):
            # Try to find it with slight variations
            for city_name, city_list in self.cities.items():
                if text_lower in city_name or city_name in text_lower:
                    return city_list
        
        return None
    
    def _extract_from_header(self, text: str) -> Optional[LocationResult]:
        """
        Extract location from header with improved patterns.
        Focuses on first ~1000 chars where contact info typically appears.
        """
        header = text[:1000]
        debug_info = {"header_length": len(header)}
        
        # Pattern 1: Explicit location labels
        location_patterns = [
            # "Location: City, State, Country"
            r'(?:location|address|residence|based in|lives? in|current location)[\s:]+([^\n]{10,100})',
            # "City, State, Country" on its own line
            r'(?:^|\n)([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?(?:,\s*[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?){1,3})(?:\n|$)',
            # Phone/Email followed by location
            r'(?:phone|email|mobile).*?\n\s*([A-Z][a-z]+(?:,\s*[A-Z][a-z]+){1,2})',
        ]
        
        for pattern in location_patterns:
            matches = re.finditer(pattern, header, re.IGNORECASE | re.MULTILINE)
            for match in matches:
                location_text = match.group(1).strip()
                debug_info["pattern_matched"] = pattern[:50]
                debug_info["raw_match"] = location_text
                
                result = self._parse_location_string(location_text)
                if result and result.confidence > 0.6:
                    result.method = "header_pattern"
                    result.debug_info = debug_info
                    return result
        
        # Pattern 2: Look for Indian state + city combinations
        lines = header.split('\n')[:15]  # First 15 lines
        for line in lines:
            # Skip lines with too many words (likely not location)
            if len(line.split()) > 8:
                continue
                
            # Look for Indian location patterns
            india_match = re.search(r'([A-Za-z\s]+),\s*([A-Za-z\s]+),?\s*(India|IN|Bharat)', line, re.I)
            if india_match:
                city_candidate = india_match.group(1).strip()
                state_candidate = india_match.group(2).strip()
                
                # Validate
                if self._is_indian_city(city_candidate) or self._is_city(city_candidate):
                    debug_info["indian_pattern"] = line
                    return LocationResult(
                        city=city_candidate,
                        region=state_candidate if self._is_indian_state(state_candidate) else None,
                        country="India",
                        confidence=0.9,
                        method="header_indian",
                        raw_entities=[city_candidate, state_candidate, "India"],
                        debug_info=debug_info
                    )
        
        return None
    
    def _parse_location_string(self, text: str) -> Optional[LocationResult]:
        """
        Parse a location string like 'Lisbon, Portugal' or 'Mumbai, Maharashtra, India'
        Enhanced for Indian locations.
        """
        # Clean up the text
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Split by comma
        parts = [p.strip() for p in text.split(',') if p.strip()]
        if len(parts) < 2:
            return None
        
        debug_info = {"parts": parts}
        
        # Last part should be country
        country_candidate = parts[-1]
        country = self._is_country(country_candidate)
        
        if not country:
            # Maybe it's implicit India (no country mentioned)
            if len(parts) == 2 and (self._is_indian_state(parts[-1]) or self._is_indian_city(parts[0])):
                country = self.countries.get("india")
                parts.append("India")
        
        if not country:
            return None
        
        # First part should be city
        city_candidate = parts[0]
        
        # For Indian locations, validate city
        if country["iso"] == "IN":
            if not (self._is_indian_city(city_candidate) or self._is_city(city_candidate)):
                # Be more lenient for less common cities
                cities = self._is_city(city_candidate)
                if not cities:
                    # Still accept if it looks like a city name
                    if not re.match(r'^[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?$', city_candidate):
                        return None
        
        # Check gazetteer for city
        cities = self._is_city(city_candidate)
        matching_city = None
        
        if cities:
            # Find city that matches country
            for city in cities:
                if city["country"] == country["iso"]:
                    matching_city = city
                    break
            
            # If multiple cities, prefer one with higher population
            if not matching_city and len(cities) > 1:
                matching_city = max(cities, key=lambda c: c.get("population", 0))
        
        # Middle part might be region/state
        region = None
        if len(parts) >= 3:
            region_candidate = parts[-2]
            if country["iso"] == "IN" and self._is_indian_state(region_candidate):
                region = region_candidate
            else:
                region = region_candidate
        
        # Calculate confidence
        confidence = 0.7
        if matching_city:
            confidence = 0.9
        if region and country["iso"] == "IN" and self._is_indian_state(region):
            confidence += 0.05
        
        confidence = min(confidence, 0.99)
        
        return LocationResult(
            city=matching_city["name"] if matching_city else city_candidate,
            region=region,
            country=country["name"],
            confidence=confidence,
            method="structured_parse",
            raw_entities=parts,
            debug_info=debug_info
        )
    
    def _extract_with_ner(self, text: str) -> Optional[LocationResult]:
        """
        Extract location using spaCy NER with enhanced logic
        """
        # Process with spaCy - use more text for better context
        doc = self.nlp(text[:5000])  # Increased from 3000
        
        # Extract GPE (Geopolitical entities) and LOC entities
        gpe_entities = []
        for ent in doc.ents:
            if ent.label_ in ("GPE", "LOC"):
                gpe_entities.append(ent.text)
        
        if not gpe_entities:
            return None
        
        debug_info = {
            "total_entities": len(gpe_entities),
            "entities": gpe_entities[:10]
        }
        
        # Score entities by position and frequency
        entity_scores = {}
        for i, ent_text in enumerate(gpe_entities[:30]):  # Increased from 20
            # Earlier entities get higher scores
            position_score = 1.0 - (i / 30)
            
            # Boost score for known Indian locations
            if self._is_indian_city(ent_text.lower()) or self._is_indian_state(ent_text.lower()):
                position_score *= 1.5
            
            entity_scores[ent_text] = entity_scores.get(ent_text, 0) + position_score
        
        # Strategy 1: Try to find country first
        country = None
        country_text = None
        for ent_text in sorted(entity_scores.keys(), key=lambda x: entity_scores[x], reverse=True):
            c = self._is_country(ent_text)
            if c:
                country = c
                country_text = ent_text
                debug_info["country_found"] = country_text
                break
        
        # Strategy 2: If no country found, look for "India" indicators
        if not country:
            # Check for Indian locations
            has_indian_location = False
            for ent_text in gpe_entities[:10]:
                if self._is_indian_city(ent_text.lower()) or self._is_indian_state(ent_text.lower()):
                    has_indian_location = True
                    break
            
            if has_indian_location:
                country = self.countries.get("india")
                country_text = "India"
                debug_info["country_inferred"] = "from_indian_entities"
        
        if not country:
            # Last resort: check text for country mentions
            text_lower = text[:2000].lower()
            if "india" in text_lower or "indian" in text_lower:
                country = self.countries.get("india")
                country_text = "India"
                debug_info["country_inferred"] = "from_text_mention"
        
        if not country:
            return None
        
        # Try to find city in same country
        city = None
        city_text = None
        region_text = None
        
        for ent_text in gpe_entities[:15]:  # Check more entities
            if ent_text == country_text:
                continue
            
            # Check if it's a state (for India)
            if country["iso"] == "IN" and self._is_indian_state(ent_text.lower()):
                if not region_text:  # Take first state found
                    region_text = ent_text
                continue
            
            # Check if it's a city
            cities = self._is_city(ent_text)
            if cities:
                for c in cities:
                    if c["country"] == country["iso"]:
                        city = c
                        city_text = ent_text
                        debug_info["city_found"] = city_text
                        break
                if city:
                    break
        
        # Calculate confidence
        if city and region_text:
            confidence = 0.85
        elif city:
            confidence = 0.75
        elif region_text:
            confidence = 0.60
        else:
            confidence = 0.50
        
        debug_info["final_confidence"] = confidence
        
        return LocationResult(
            city=city["name"] if city else None,
            region=region_text,
            country=country["name"],
            confidence=confidence,
            method="ner_spacy",
            raw_entities=gpe_entities[:7],
            debug_info=debug_info
        )
    
    def _extract_with_regex_fallback(self, text: str) -> Optional[LocationResult]:
        """
        Regex-based fallback extraction for when NER fails.
        Particularly good for Indian resumes.
        """
        debug_info = {"method": "regex_fallback"}
        
        # Pattern: City, State, India
        indian_pattern = r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?),\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?),?\s*(?:India|IN)\b'
        matches = re.findall(indian_pattern, text[:2000])
        
        for city_cand, state_cand in matches:
            if self._is_indian_city(city_cand.lower()) or self._is_city(city_cand.lower()):
                debug_info["pattern"] = f"{city_cand}, {state_cand}, India"
                return LocationResult(
                    city=city_cand,
                    region=state_cand if self._is_indian_state(state_cand.lower()) else None,
                    country="India",
                    confidence=0.70,
                    method="regex_indian",
                    raw_entities=[city_cand, state_cand, "India"],
                    fallback_used=True,
                    debug_info=debug_info
                )
        
        # Pattern: Just look for Indian cities
        for city_name in self.INDIAN_CITIES:
            # Case-insensitive word boundary search
            pattern = r'\b' + re.escape(city_name) + r'\b'
            if re.search(pattern, text[:2000], re.IGNORECASE):
                # Capitalize properly
                city_proper = city_name.title()
                debug_info["city_mention"] = city_proper
                return LocationResult(
                    city=city_proper,
                    region=None,
                    country="India",
                    confidence=0.60,
                    method="regex_city_mention",
                    raw_entities=[city_proper],
                    fallback_used=True,
                    debug_info=debug_info
                )
        
        return None
    
    def extract_from_resume(self, resume_text: str, debug: bool = False) -> Optional[LocationResult]:
        """
        Extract location from resume text with multi-stage fallback.
        
        Args:
            resume_text: Full text of resume/CV
            debug: Print debug information
            
        Returns:
            LocationResult or None
        """
        if not resume_text or len(resume_text) < 20:
            if debug:
                print("  ⚠️  Text too short (<20 chars)")
            return None
        
        # Normalize text
        text = self._normalize_text(resume_text)
        
        if debug:
            print(f"  🔍 NER processing {len(text)} chars...")
        
        # Stage 1: Try header patterns first (highest confidence)
        result = self._extract_from_header(text)
        if result and result.confidence >= 0.80:
            if debug:
                print(f"  ✅ Header: {format_location(result)} (conf={result.confidence:.2f}, method={result.method})")
            return result
        
        # Stage 2: Try full NER extraction
        ner_result = self._extract_with_ner(text)
        if ner_result and ner_result.confidence >= 0.70:
            if debug:
                print(f"  ✅ NER: {format_location(ner_result)} (conf={ner_result.confidence:.2f})")
                if ner_result.debug_info:
                    print(f"      Debug: {ner_result.debug_info}")
            return ner_result
        
        # Stage 3: Use NER result if it's better than header (even with lower confidence)
        if ner_result and (not result or ner_result.confidence > result.confidence):
            if debug:
                print(f"  ✅ NER (fallback): {format_location(ner_result)} (conf={ner_result.confidence:.2f})")
            return ner_result
        
        # Stage 4: Regex fallback
        regex_result = self._extract_with_regex_fallback(text)
        if regex_result:
            if debug:
                print(f"  ✅ Regex: {format_location(regex_result)} (conf={regex_result.confidence:.2f})")
            return regex_result
        
        # Stage 5: Use whatever we found
        if result:
            if debug:
                print(f"  ⚠️  Weak result: {format_location(result)} (conf={result.confidence:.2f})")
            return result
        
        if debug:
            print("  ❌ No location found")
        
        return None


def format_location(result: LocationResult) -> str:
    """Format location result as string"""
    if not result:
        return ""
    
    parts = []
    if result.city:
        parts.append(result.city)
    if result.region:
        parts.append(result.region)
    if result.country:
        parts.append(result.country)
    
    return ", ".join(parts)


# Test function
if __name__ == "__main__":
    print("=" * 60)
    print("Testing ImprovedLocationNER")
    print("=" * 60)
    
    ner = ImprovedLocationNER()
    
    test_texts = [
        # Test 1: Standard format
        """
        John Smith
        Software Engineer
        Mumbai, Maharashtra, India
        Email: john@example.com
        Phone: +91-9876543210
        """,
        
        # Test 2: Header with location label
        """
        RAMESH KUMAR
        Senior QA Engineer
        Location: Chennai, Tamil Nadu, India
        ramesh.kumar@email.com | +91-9988776655
        """,
        
        # Test 3: Sparse text (like poor PDF extraction)
        """
        Bangalore India
        Python Developer
        5 years experience
        """,
        
        # Test 4: No explicit location label
        """
        PRIYA SHARMA
        Data Analyst
        Pune, India
        
        EXPERIENCE
        Data Analyst at Tech Corp
        """,
        
        # Test 5: State only
        """
        VIJAY RAO
        West Bengal, India
        Full Stack Developer
        """,
    ]
    
    for i, text in enumerate(test_texts, 1):
        print(f"\n{'─' * 60}")
        print(f"TEST {i}")
        print(f"{'─' * 60}")
        print(f"Text preview: {' '.join(text.split()[:15])}...")
        print()
        
        result = ner.extract_from_resume(text, debug=True)
        
        if result:
            print(f"\n📍 RESULT: {format_location(result)}")
            print(f"   Confidence: {result.confidence:.2f}")
            print(f"   Method: {result.method}")
            if result.fallback_used:
                print(f"   ⚠️  Fallback used")
        else:
            print("\n❌ No location found")
    
    print(f"\n{'=' * 60}")
    print("Testing complete!")
    print(f"{'=' * 60}")
