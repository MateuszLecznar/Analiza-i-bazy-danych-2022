import pytest
import app

from app import *
test_data=[[3,5,3,1,2,5,18],[3,4,6,1,2,3,5,8,9,10,12],[34,6,2,1,5,8]]
expected=[[1,2,3,3,5,5,18],[1,2,3,3,4,5,6,8,9,10,12],[1,2,5,6,8,34]]

def test_selection_sort_asc_0():
    got=selection_sort_asc(test_data[0])
    assert got == expected[0]

def test_selection_sort_asc_1():
    got=selection_sort_asc(test_data[1])
    assert got == expected[1]

def test_selection_sort_asc_2():
    got=selection_sort_asc(test_data[2])
    assert got == expected[2]


testdata = [
    ('There is a duck in this text', 'duck', True),
    ('There is nothing here', 'duck', False)
    ]

@pytest.mark.parametrize('sample, word, expected_output', testdata)
def test_text_contain_word(sample, word, expected_output):

    assert text_contain_word(word, sample) == expected_output



testdata_ = ["I think today will be a great day","I do not think this will turn out well"]

@pytest.mark.parametrize('sample', testdata_)
def test_extract_sentiment(sample):

    sentiment = extract_sentiment(sample)

    assert sentiment > 0