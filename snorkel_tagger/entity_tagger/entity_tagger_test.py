import entity_tagger as tagger
import json


def test_parsing_all_words():
    with open("tests/resources/generic_hocr.json", "r") as f:
        generic_hocr = json.load(f)["Words"]
    words = tagger.parse_words(generic_hocr)
    with open("tests/resources/generic_hocr_parsed_.json", "r") as f:
        generic_hocr_parsed = json.load(f)
    assert words == generic_hocr_parsed

    assert type(words) == list
    for word in words:
        assert type(word) == dict
        assert "word_id" in word
        assert "text" in word
        assert "bounding_box" in word
        assert "confidence" in word

        assert type(word["word_id"]) == str
        assert type(word["text"]) == str
        assert type(word["bounding_box"]) == list
        assert type(word["confidence"]) == float


def test_blob_maker():
    with open("tests/resources/generic_hocr_parsed.json", "r") as f:
        generic_hocr_parsed = json.load(f)
    assert type(tagger.make_blob(generic_hocr_parsed)) == str


def test_comprehend_parsing():
    output = {
        "Score": 0.998681902885437,
        "Type": "PERSON",
        "Text": "Chris Addy",
        "BeginOffset": 18,
        "EndOffset": 28,
    }

    expected_parsed_output = [
        {
            "entity_score": 0.998681902885437,
            "entity_id": "1234",
            "entity_type": "PERSON",
            "text": "Chris",
            "string_index": 18,
        },
        {
            "entity_id": "1234",
            "entity_score": 0.998681902885437,
            "entity_type": "PERSON",
            "text": "Addy",
            "string_index": 24,
        },
    ]

    parsed_output = tagger.parse_comprehend_output(output, "1234")

    assert parsed_output == expected_parsed_output


# def test_comprehend():
#     with open("tests/resources/entities.json", "r") as f:
#         parsed_entities = json.load(f)

#     test_string = "Hello, my name is Christopher William Addy. I have $20.30 in Amazon stock. My pal, Akshay Joshi, who is 5' 9\" tall, or 174.3 cm, is a fan of Chelsea, will hopefully review and approve my PR on Github within the next 100.2828 minutes."
#     entities = tagger.comprehend_entities(test_string)

#     entities_no_id = [
#         {k: v for k, v in d.items() if k != "entity_id"} for d in entities
#     ]
#     parsed_entities_no_id = [
#         {k: v for k, v in d.items() if k != "entity_id"} for d in parsed_entities
#     ]

#     assert entities_no_id == parsed_entities_no_id


def test_zipping():
    with open("tests/resources/generic_zipped_2.json", "r") as f:
        generic_zipped = json.load(f)

    with open("tests/resources/generic_hocr_entities.json", "r") as f:
        generic_hocr_entities = json.load(f)

    with open("tests/resources/generic_hocr_parsed.json", "r") as f:
        generic_hocr_parsed = json.load(f)

    zipped = tagger.zip_words_entities(generic_hocr_parsed, generic_hocr_entities)
    assert zipped == generic_zipped


def test_make_blob():
    print("test_make_blob")
    with open("tests/resources/make_blob.json", "r") as f:
        words = json.load(f)
    blob = tagger.make_blob(words)
    print(blob)
    # Open a file: file
    file = open("tests/resources/expected_make_blob.txt", mode="r")
    # read all lines at once
    expected_blob = file.read()
    # close the file
    file.close()
    print(expected_blob)
    assert blob == expected_blob


# def test_all_components():
#     with open("tests/resources/generic_zipped.json", "r") as f:
#         generic_zipped = json.load(f)

#     with open("tests/resources/generic_hocr.json", "r") as f:
#         generic_hocr = json.load(f)["hocr"]

#     words = tagger.parse_words(generic_hocr)

#     blob = tagger.make_blob(words)

#     entities = tagger.comprehend_entities(blob)
#     entities = [{k: v for k, v in d.items() if k != "entity_id"} for d in entities]

#     zipped = tagger.zip_words_entities(words, entities)

#     assert zipped["entities"] == [
#         {k: v for k, v in z.items() if k != "entity_id"}
#         for z in generic_zipped["entities"]
#     ]


# not really a unit test, since it has to go through comprehend, need to be run while auth'd
# def test_version_is_included():
#     with open("tests/resources/generic_hocr.json", "r") as f:
#         generic_hocr = json.load(f)

#     response = tagger.handler(generic_hocr, context={})["body"]

#     with open("tests/resources/generic_response.json", "w") as f:
#         json.dump(response, f)

#     assert "entities" in response
#     assert "version" in response
#     assert response["version"].split("-")[:2] == ["2019", "10"]
global entitiesInput
global expectedOtpt

with open("tests/resources/ssn_entities_input.json", "r") as r:
    entitiesInput = json.load(r)

with open("tests/resources/ssn_entities_output.json", "r") as r:
    expectedOtpt = json.load(r)

def test_append_ssn_formats_correct():
    testInput = "113 XXX-XX-9229 xxx-xx-9229 Xxx-xx-7655 ***-**-7655 123-22-7655 Abc-de-fghi Heavy-Water-Test"
    otpt = tagger.append_ssn_formats(testInput,entitiesInput)
    assert otpt == expectedOtpt

def test_append_ssn_formats_incorrect():
    testInput = ["113" ,"XXX-XX-9229" ,"xxx-xx-9229" ,"Xxx-xx-7655" ,"***-**-7655" ,"123-22-7655" ,"Abc-de-fghi", "Heavy-Water-Test"]
    otpt = tagger.append_ssn_formats(testInput,entitiesInput)
    assert otpt == entitiesInput