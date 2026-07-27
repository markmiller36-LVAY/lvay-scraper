from school_database import get_school


def test_harrisonburg_class_is_sport_specific():
    assert get_school("Harrisonburg", "baseball")["class"] == "C"
    assert get_school("Harrisonburg", "baseball")["division"] == "Class C"
    assert get_school("Harrisonburg", "softball")["class"] == "C"
    assert get_school("Harrisonburg", "softball")["division"] == "Class C"
    assert get_school("Harrisonburg", "girls_basketball")["class"] == "B"
    assert get_school("Harrisonburg")["class"] == "B"
