import unittest
from math import ceil

class TestPagination(unittest.TestCase):
    def test_pagination_valid(self):
        """Test valid pagination logic."""
        results = list(range(100))  # Mock results
        page_size = 10
        page = 2
        total_pages = ceil(len(results) / page_size)
        start_index = (page - 1) * page_size
        end_index = start_index + page_size
        paginated_results = results[start_index:end_index]
        self.assertEqual(paginated_results, list(range(10, 20)))
        self.assertEqual(total_pages, 10)

    def test_pagination_invalid_page(self):
        """Test invalid page number."""
        results = list(range(100))  # Mock results
        page_size = 10
        page = 11  # Invalid page
        total_pages = ceil(len(results) / page_size)
        self.assertTrue(page > total_pages)

    def test_pagination_empty_results(self):
        """Test pagination with empty results."""
        results = []  # Empty results
        page_size = 10
        page = 1
        total_pages = ceil(len(results) / page_size)
        self.assertEqual(total_pages, 0)
        self.assertEqual(results, [])

    def test_pagination_edge_case(self):
        """Test edge case where results fit exactly into pages."""
        results = list(range(20))  # Mock results
        page_size = 10
        page = 2
        total_pages = ceil(len(results) / page_size)
        start_index = (page - 1) * page_size
        end_index = start_index + page_size
        paginated_results = results[start_index:end_index]
        self.assertEqual(paginated_results, list(range(10, 20)))
        self.assertEqual(total_pages, 2)

if __name__ == "__main__":
    unittest.main()