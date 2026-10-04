import os
import shutil
import tempfile
import unittest
from datetime import date
from unittest import mock

os.environ.setdefault("DB_PATH", "/tmp/lvay_social_test.db")

import server
import social_poster as sp


class SocialHelperTests(unittest.TestCase):
    def test_score_follows_win_loss(self):
        self.assertEqual(sp.parse_score("7-49", "L"), (7, 49))
        self.assertEqual(sp.parse_score("49-7", "L"), (7, 49))  # flipped row
        self.assertEqual(sp.parse_score("14-35", "W"), (35, 14))
        self.assertIsNone(sp.parse_score("", "W"))

    def test_division_labels(self):
        self.assertEqual(sp.division_label("Non-Select Division 1"), "Non-Select Division I")
        self.assertEqual(sp.division_short("Select Division IV"), "S IV")
        self.assertEqual(sp.division_short("Non-Select Division 2"), "NS II")

    def test_daily_plan(self):
        self.assertEqual(sp.plan_for(date(2026, 10, 10)), ["finals-big", "finals-roundup"])  # Sat
        self.assertEqual(sp.plan_for(date(2026, 10, 11)), ["ratings"])  # Sun
        self.assertEqual(sp.plan_for(date(2026, 10, 12)), ["standings-5A"])  # Mon
        self.assertEqual(sp.plan_for(date(2026, 10, 16)), ["standings-1A"])  # Fri

    def test_finals_are_deduped_and_home_team_is_known(self):
        schools = [
            {"school": "Airline", "class_": "5A", "record": "5-0", "games": [
                {"week": 6, "game_date": "10/9/2026", "opponent": "Parkway", "result": "W",
                 "score": "45-22", "home_away": "H", "is_district": True}]},
            {"school": "Parkway", "class_": "5A", "record": "3-2", "games": [
                {"week": 6, "game_date": "10/9/2026", "opponent": "Airline", "result": "L",
                 "score": "22-45", "home_away": "A", "is_district": True}]},
        ]
        finals = sp.collect_finals(schools, date(2026, 10, 7), date(2026, 10, 10))
        self.assertEqual(len(finals), 1)
        game = finals[0]
        self.assertEqual((game["winner"], game["winner_pts"], game["loser_pts"]), ("Airline", 45, 22))
        self.assertEqual(game["home"], "Airline")

    def test_review_token_is_per_post(self):
        self.assertNotEqual(sp.review_token(1), sp.review_token(2))


class SocialFlowTests(unittest.TestCase):
    """Builds real posts from the 2024 archive and runs the review page."""

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.env = mock.patch.dict(os.environ, {
            "SOCIAL_DIR": self.dir, "PIPELINE_TOKEN": "secret-token",
            "RESEND_API_KEY": "", "META_PAGE_TOKEN": "", "META_PAGE_ID": "",
            "META_IG_USER_ID": "", "SOCIAL_AUTO_APPROVE": "",
        })
        self.env.start()
        import social_graphics
        social_graphics.LOGOS.index = {}
        social_graphics.LOGOS.tiles = {}
        social_graphics.LOGOS.local = {'_offline': ''}
        self.client = server.app.test_client()
        conn = sp.db()
        conn.execute("DELETE FROM social_posts")
        conn.commit()
        conn.close()

    def tearDown(self):
        self.env.stop()
        shutil.rmtree(self.dir, ignore_errors=True)

    def test_run_requires_token(self):
        self.assertEqual(self.client.post("/api/social/run").status_code, 401)

    def test_saturday_builds_posts_and_waits_for_approval(self):
        feed = sp.Feed(season=2024, client=self.client)
        result = sp.run_for_day(date(2024, 10, 12), feed=feed)
        posts = {p["kind"]: p for p in result["posts"]}
        self.assertIn("id", posts["finals-roundup"])
        self.assertLessEqual(posts["finals-roundup"]["slides"], sp.IG_MAX_SLIDES)
        self.assertEqual(posts["finals-big"]["status"], "pending")

        # same day again = no duplicates
        again = sp.run_for_day(date(2024, 10, 12), feed=feed)
        self.assertFalse(any(p.get("new") for p in again["posts"]))

        post = sp.get_post(posts["finals-big"]["id"])
        image_path = "/social/img/" + __import__("json").loads(post["slides"])[0]
        self.assertEqual(self.client.get(image_path).status_code, 200)
        self.assertEqual(self.client.get("/social/img/../x.jpg").status_code, 404)

        bad = self.client.get(f"/social/review/{post['id']}?t=wrong")
        self.assertEqual(bad.status_code, 404)
        url = f"/social/review/{post['id']}?t={sp.review_token(post['id'])}"
        self.assertIn(b"Approve &amp; post", self.client.get(url).data)
        # Approving without Meta settings records the OK but posts nothing.
        self.client.post(url, data={"action": "approve", "caption": "Edited",
                                    "t": sp.review_token(post["id"])})
        post = sp.get_post(post["id"])
        self.assertEqual(post["status"], "approved")
        self.assertEqual(post["caption"], "Edited")
        self.assertIn("aren't connected", post["error"])

    def test_skip(self):
        feed = sp.Feed(season=2024, client=self.client)
        result = sp.run_for_day(date(2024, 10, 13), feed=feed)
        post_id = result["posts"][0]["id"]
        token = sp.review_token(post_id)
        self.client.post(f"/social/review/{post_id}", data={"action": "skip", "t": token})
        self.assertEqual(sp.get_post(post_id)["status"], "skipped")


if __name__ == "__main__":
    unittest.main()
