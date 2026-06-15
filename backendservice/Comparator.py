from abc import ABC, abstractmethod
import re
from difflib import SequenceMatcher

class BaseComparator(ABC):
    @abstractmethod
    def compare(self, v1, v2) -> bool:
        pass

class ExactMatch(BaseComparator):
    def compare(self, v1, v2) -> bool:
        return str(v1).strip() == str(v2).strip()

class RegexpMatch(BaseComparator):
    def compare(self, pattern, value) -> bool:
        if pattern is None or value is None:
            return False
        try:
            return re.fullmatch(pattern, str(value)) is not None
        except re.error:
            return False

class CheckNoMatch:
    def __init__(self, value):
        self.regex = re.compile(r'\bcheque\b.*\bno\.?.?\s*[-|]?\s*(\d+)', re.I)
        self.value = value

    def getCheckNo(self):
        if self.value is None:
            return None
        m = self.regex.search(str(self.value))
        return m.group(1) if m else None

class FuzzyMatch(BaseComparator):
    def __init__(self, threshold=0.70):
        self.threshold = threshold

    def compare(self, v1, v2) -> bool:
        if v1 is None or v2 is None:
            return False
        similarity = SequenceMatcher(None, str(v1), str(v2)).ratio()
        return similarity >= self.threshold
