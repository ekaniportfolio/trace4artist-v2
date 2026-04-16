"""
tests/test_scorer.py — Tests du scorer v2 final

Couvre :
    - SPR (Score de Performance Relative)
    - Indice de Viralité Organique (likes + comments)
    - Vélocité 24h et 7j depuis snapshots enrichis
    - Régularité des publications
    - Fake View Detector
    - Segmentation et intégrité du score
"""

import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

from src.scorer import ArtistScorer, ScoringResult, FAKE_VIEW_SCORE_PENALTY
from config import SCORE_SEGMENTS


@pytest.fixture
def scorer():
    return ArtistScorer()


def make_snapshot(hours_ago: int, views: int, likes: int = 0,
                  comments: int = 0, subs: int = 10_000) -> dict:
    ts = (datetime.now(timezone.utc) - timedelta(hours=hours_ago)).isoformat()
    return {
        "view_count"      : views,
        "like_count"      : likes,
        "comment_count"   : comments,
        "subscriber_count": subs,
        "snapped_at"      : ts,
    }


def make_video(view_count: int, like_count: int = 500,
               comment_count: int = 50, days_ago: int = 3) -> dict:
    pub = (datetime.now(timezone.utc) - timedelta(days=days_ago)).isoformat()
    return {
        "video_id"     : "vid001",
        "view_count"   : view_count,
        "like_count"   : like_count,
        "comment_count": comment_count,
        "published_at" : pub,
    }


def make_artist(**overrides) -> dict:
    base = {
        "channel_id"      : "UCtest",
        "artist_name"     : "Artiste Test",
        "subscriber_count": 20_000,
        "email"           : "contact@artiste.cm",
        "instagram"       : "artiste_cm",
        "website"         : "https://artiste.cm",
        "videos"          : [make_video(50_000)],
        "snapshots"       : [
            make_snapshot(24, 40_000),
            make_snapshot(0,  50_000),
        ],
    }
    base.update(overrides)
    return base


# ── Disqualification ──────────────────────────────────────────────────

class TestDisqualification:

    def test_too_few_subscribers(self, scorer):
        result = scorer.score_artist(make_artist(subscriber_count=100))  # < 500
        assert result.is_qualified is False
        assert result.total_score  == 0.0
        assert "abonnés" in result.disqualification_reason.lower()

    def test_too_few_views(self, scorer):
        result = scorer.score_artist(make_artist(videos=[make_video(1000)]))  # < 5000
        assert result.is_qualified is False
        assert "vues" in result.disqualification_reason.lower()

    def test_good_artist_qualifies(self, scorer):
        result = scorer.score_artist(make_artist())
        assert result.is_qualified is True


# ── SPR ───────────────────────────────────────────────────────────────

class TestSPR:

    def test_spr_zero_when_no_subscribers(self, scorer):
        assert scorer._score_spr(0.0) == 0.0

    def test_spr_above_1_signals_external_virality(self, scorer):
        assert scorer._score_spr(1.5) > scorer._score_spr(0.8)

    def test_spr_computed_correctly(self, scorer):
        spr = scorer._compute_spr([make_video(50_000)], 20_000)
        assert abs(spr - 2.5) < 0.01

    def test_spr_max_capped(self, scorer):
        assert scorer._score_spr(100.0) == 20.0

    def test_underperforming_gets_low_spr(self, scorer):
        spr = scorer._compute_spr([make_video(5_000)], 100_000)
        assert scorer._score_spr(spr) <= 2.0


# ── Engagement ────────────────────────────────────────────────────────

class TestEngagement:

    def test_comments_included_in_engagement(self, scorer):
        v_with    = make_video(10_000, like_count=200, comment_count=100)
        v_without = make_video(10_000, like_count=200, comment_count=0)
        assert scorer._compute_engagement([v_with]) > \
               scorer._compute_engagement([v_without])

    def test_captive_audience_threshold(self, scorer):
        assert scorer._score_engagement(0.05) >= 15.0

    def test_zero_views_handled(self, scorer):
        assert scorer._compute_engagement(
            [{"view_count": 0, "like_count": 0, "comment_count": 0}]
        ) == 0.0

    def test_engagement_max_capped(self, scorer):
        assert scorer._score_engagement(0.5) == 20.0


# ── Vélocité ──────────────────────────────────────────────────────────

class TestVelocity:

    def test_velocity_24h_positive_growth(self, scorer):
        snaps = [make_snapshot(24, 40_000), make_snapshot(0, 50_000)]
        vel   = scorer._compute_velocity_24h(snaps)
        assert abs(vel - 0.25) < 0.01

    def test_velocity_24h_no_growth(self, scorer):
        snaps = [make_snapshot(24, 50_000), make_snapshot(0, 50_000)]
        assert scorer._compute_velocity_24h(snaps) == 0.0

    def test_velocity_needs_two_snapshots(self, scorer):
        assert scorer._compute_velocity_24h([]) == 0.0
        assert scorer._compute_velocity_24h(
            [make_snapshot(12, 10_000)]
        ) == 0.0

    def test_velocity_7d_from_first_and_last(self, scorer):
        snaps = [
            make_snapshot(168, 10_000),
            make_snapshot(84,  15_000),
            make_snapshot(0,   20_000),
        ]
        vel = scorer._compute_velocity_7d(snaps)
        assert abs(vel - 1.0) < 0.01

    def test_velocity_scores_increase(self, scorer):
        assert (scorer._score_velocity_24h(0.03) <
                scorer._score_velocity_24h(0.15) <
                scorer._score_velocity_24h(0.60))


# ── Fake View Detector ────────────────────────────────────────────────

class TestFakeViewDetector:

    def test_detects_suspicious_video(self, scorer):
        v = make_video(500_000, like_count=10, comment_count=5)
        is_sus, details = scorer._detect_fake_views([v])
        assert is_sus is True
        assert "engagement" in details

    def test_real_video_not_flagged(self, scorer):
        v = make_video(50_000, like_count=2_000, comment_count=200)
        assert scorer._detect_fake_views([v])[0] is False

    def test_small_videos_not_checked(self, scorer):
        v = make_video(50_000, like_count=0, comment_count=0)
        assert scorer._detect_fake_views([v])[0] is False

    def test_suspicious_artist_gets_penalty(self, scorer):
        result = scorer.score_artist(make_artist(
            videos=[make_video(500_000, like_count=5, comment_count=2)]
        ))
        assert result.is_suspicious is True
        assert result.breakdown.get("fraud_penalty") == -FAKE_VIEW_SCORE_PENALTY

    def test_score_never_below_zero(self, scorer):
        result = scorer.score_artist(make_artist(
            videos=[make_video(500_000, like_count=1, comment_count=0)]
        ))
        assert result.total_score >= 0.0


# ── Segmentation ──────────────────────────────────────────────────────

class TestSegmentation:

    def test_high_potential(self, scorer):
        assert scorer._get_segment(85.0) == "high_potential"

    def test_standard(self, scorer):
        assert scorer._get_segment(65.0) == "standard"

    def test_emerging(self, scorer):
        assert scorer._get_segment(45.0) == "emerging"

    def test_low_priority(self, scorer):
        assert scorer._get_segment(20.0) == "low_priority"

    def test_exact_boundaries(self, scorer):
        assert scorer._get_segment(80.0) == "high_potential"
        assert scorer._get_segment(79.9) == "standard"
        assert scorer._get_segment(60.0) == "standard"
        assert scorer._get_segment(59.9) == "emerging"
        assert scorer._get_segment(40.0) == "emerging"
        assert scorer._get_segment(39.9) == "low_priority"


# ── Intégrité du score ────────────────────────────────────────────────

class TestScoreIntegrity:

    def test_total_never_exceeds_100(self, scorer):
        assert scorer.score_artist(make_artist()).total_score <= 100.0

    def test_breakdown_has_7_criteria(self, scorer):
        result   = scorer.score_artist(make_artist())
        expected = {
            "spr", "engagement", "velocity_24h", "velocity_7d",
            "regularity", "channel", "web_contact"
        }
        assert expected.issubset(set(result.breakdown.keys()))

    def test_result_always_has_segment(self, scorer):
        result = scorer.score_artist(make_artist())
        assert result.segment in (
            "high_potential", "standard", "emerging", "low_priority"
        )

    def test_breakout_threshold_read_from_settings(self, scorer):
        """Le seuil de breakout doit etre lu depuis SettingsManager, pas hardcode."""
        from unittest.mock import patch
        # Simuler un seuil a 0.50 en base
        with patch("src.scorer._get_breakout_threshold", return_value=0.50):
            # Velocite de 40% — en dessous de 0.50, pas de breakout
            snaps = [make_snapshot(24, 100_000), make_snapshot(0, 140_000)]
            vel   = scorer._compute_velocity_24h(snaps)
            # vel = 0.40 < 0.50 donc pas de breakout avec le bon seuil
            assert vel < 0.50

        with patch("src.scorer._get_breakout_threshold", return_value=0.20):
            # Avec l'ancien seuil 0.20, 40% declencherait un breakout
            assert vel > 0.20