import os
import unittest
from unittest.mock import Mock, patch

import website_refresh as refresh

ROW = dict(school='Newman', division='Select Division III', class_='2A', wins=2, losses=1, ties=0,
           games_played=3, power_rating=16.11, strength_factor=6.0)
HTML = '<table class="lvay-rtbl"><tbody><tr><td>1</td><td>Newman</td><td>2A</td><td>2-1</td><td>3</td><td>16.11</td><td>6.00</td></tr></tbody></table>'
ENV = {'WEBSITE_REFRESH_ENABLED': 'true', 'WORDPRESS_USERNAME': 'test',
       'WORDPRESS_APP_PASSWORD': 'secret-for-test'}


def response(payload=None, text=''):
    value = Mock(status_code=200, text=text)
    value.json.return_value = payload
    return value


class WebsiteRefreshTests(unittest.TestCase):
    def test_all_public_values_must_match(self):
        refresh.verify_football(HTML, [ROW])
        for old, new in [('16.11', '15.11'), ('2-1', '1-1'), ('6.00', '5.00'), ('>3<', '>2<'), ('2A', '3A')]:
            with self.subTest(new=new), self.assertRaises(ValueError):
                refresh.verify_football(HTML.replace(old, new), [ROW])

    def test_missing_and_duplicate_schools_fail(self):
        for html in ['', HTML + HTML]:
            with self.assertRaises(ValueError):
                refresh.verify_football(html, [ROW])

    @patch.dict(os.environ, {}, clear=True)
    def test_disabled_is_explicit_and_does_not_call_network(self):
        with patch.object(refresh.requests, 'post') as post:
            self.assertIn('Not enabled', refresh.refresh_website(['football']))
            post.assert_not_called()

    @patch.dict(os.environ, {'WEBSITE_REFRESH_ENABLED': 'true'}, clear=True)
    def test_missing_credentials_fail_closed(self):
        with self.assertRaisesRegex(RuntimeError, 'credentials'):
            refresh.refresh_website(['football'])

    @patch.dict(os.environ, ENV, clear=True)
    @patch('scraper.resolve_season_year', return_value=2026)
    def test_waits_for_public_cache_without_sending_credentials_to_public_reads(self, season):
        with patch.object(refresh.requests, 'post', return_value=response({'purge_requested': True})) as post, patch.object(refresh.requests, 'get', side_effect=[response({'season': 2026, 'rankings': [ROW]}), response(text=HTML.replace('16.11', '15.11')), response(text=HTML)]) as get, patch.object(refresh.time, 'sleep') as sleep:
            self.assertIn('all 1 public football ratings verified', refresh.refresh_website(['football']))
            sleep.assert_called_once_with(15)
            self.assertFalse(post.call_args.kwargs['allow_redirects'])
            for call in get.call_args_list:
                self.assertNotIn('auth', call.kwargs)
            self.assertEqual(get.call_args.args[0], refresh.SITE + '/power-rankings/')

    @patch.dict(os.environ, ENV, clear=True)
    @patch('scraper.resolve_season_year', return_value=2026)
    def test_stale_public_page_fails_run(self, season):
        with patch.object(refresh.requests, 'post', return_value=response({'purge_requested': True})), patch.object(refresh.requests, 'get', side_effect=[response({'season': 2026, 'rankings': [ROW]})] + [response(text='old page')] * 13), patch.object(refresh.time, 'sleep'):
            with self.assertRaisesRegex(RuntimeError, 'remain stale'):
                refresh.refresh_website(['football'])

    @patch.dict(os.environ, ENV, clear=True)
    def test_login_redirect_is_not_success(self):
        reply = response()
        reply.status_code = 302
        with patch.object(refresh.requests, 'post', return_value=reply):
            with self.assertRaisesRegex(RuntimeError, 'acknowledge'):
                refresh.refresh_website([])


if __name__ == '__main__':
    unittest.main()
