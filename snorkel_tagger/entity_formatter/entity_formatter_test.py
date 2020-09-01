import entity_formatter as formatter
import json
import dateutil
import preprocess


with open("tests/resources/input.json", "r") as f:
    input_entities = json.load(f)

with open("tests/resources/input_page_9.json", "r") as f:
    input_entities_page_9 = json.load(f)

with open("tests/resources/output_page_9.json", "r") as f:
    output_page_9 = json.load(f)

with open("tests/resources/output_filtered.json", "r") as f:
    output_filtered = json.load(f)

with open("tests/resources/aggregate_by_id.json", "r") as f:
    aggregate_by_id = json.load(f)

with open("tests/resources/group_by_type.json", "r") as f:
    group_by_type = json.load(f)

with open("tests/resources/reduce_updated.json", "r") as f:
    reduce_ = json.load(f)

with open("tests/resources/input_large.json", "r") as f:
    input_large = json.load(f)

with open("tests/resources/output_large_filtered_.json", "r") as f:
    output_large_filtered = json.load(f)

with open("tests/resources/reduce_w2.json", "r") as f:
    reduce_w2 = json.load(f)

with open("tests/resources/types_w2.json", "r") as f:
    types_w2 = json.load(f)

with open("tests/resources/types.json", "r") as f:
    types = json.load(f)

with open("tests/resources/input_w2.json", "r") as f:
    input_w2 = json.load(f)

with open("tests/resources/output_w2.json", "r") as f:
    output_w2 = json.load(f)

with open("tests/resources/output_aggregation.json", "r") as f:
    output_aggregation = json.load(f)

with open("tests/resources/cleaned_entities.json", "r") as f:
    cleaned_entities_input = json.load(f)

with open("tests/resources/mapped_out.json", "r") as f:
    mapped_out = json.load(f)

with open("tests/resources/address_out.json", "r") as f:
    address_out = json.load(f)

with open("tests/resources/address_map.json", "r") as f:
    address_map = json.load(f)


def test_page_number_replace():
    replaced_page_number = preprocess.replace_page_number(cleaned_entities_input, "9")
    assert replaced_page_number == input_entities_page_9
    output_page = formatter.format_entities(input_entities, "9")
    assert output_page["body"] == output_page_9["body"]


def test_entity_aggregation():
    cleaned = preprocess.entity_cleaning(input_entities)
    with_page_number = preprocess.replace_page_number(cleaned, "9")
    mapped_entities = preprocess.map_entities(with_page_number)
    assert mapped_entities == aggregate_by_id
    reduced_entities = preprocess.reduce_entities(mapped_entities)
    assert reduced_entities == reduce_
    groupped_entities = formatter.group_entities(reduced_entities)
    assert groupped_entities == group_by_type
    translate_entities = formatter.translate_entities(groupped_entities, "9")
    assert translate_entities == output_aggregation


def validate(in_req, out_exp):
    cleaned = preprocess.entity_cleaning(in_req)
    with_page_number = preprocess.replace_page_number(cleaned, "9")
    mapped_entities = preprocess.map_entities(with_page_number)
    reduced_entities = preprocess.reduce_entities(mapped_entities)
    groupped_entities = formatter.group_entities(reduced_entities)
    translated_entities = formatter.translate_entities(groupped_entities, "9")
    if "PERSON NAME" in translated_entities.keys():
        translated_entities = formatter.add_full_name_entities(translated_entities)
    assert translated_entities == out_exp


def entity_type_(red, exp):
    group_names = {}
    for entity_id, entity in reduce_w2.items():
        group_names[entity_id] = formatter.get_entity_type(
            entity["entity_type"], entity
        )
        assert group_names[entity_id] == types_w2[entity_id]


def test_get_entity_type():
    entities_to_assert = []
    entity_types = [
        "YEAR",
        "PERSON",
        "LOCATION",
        "ORGANIZATION",
        "COMMERCIAL_ITEM",
        "EVENT",
        "TITLE",
        "DATE",
        "QUANTITY",
        "OTHER",
        "ZIPCODE",
        "STATE",
        "ROUTE",
        "CITY",
    ]
    entities_cleaned = preprocess.entity_cleaning(input_entities)
    entities_with_page_number = preprocess.replace_page_number(entities_cleaned, "9")
    mapped_entities = preprocess.map_entities(entities_with_page_number)
    reduced_entities = preprocess.reduce_entities(mapped_entities)
    for entity in reduced_entities.values():
        entities_to_assert.append(
            formatter.get_entity_type(entity["entity_type"], entity)
        )
    for i in entities_to_assert:
        assert i in entity_types


def test_map_entities():
    entities_cleaned = preprocess.entity_cleaning(input_entities)
    entities_with_page_number = preprocess.replace_page_number(entities_cleaned, "9")
    mapped_entities = preprocess.map_entities(entities_with_page_number)
    assert mapped_out == mapped_entities


def test_format():
    text_date = "01/01"
    text_quntity = "12.8"
    text_plan = "sometext"
    value = formatter.format(text_date, "DATE")
    assert dateutil.parser.parse(text_date).strftime("%m/%d/%Y") == value
    value = formatter.format(text_quntity, "QUANTITY")
    assert (
        text_quntity.replace(",", "").replace("$", "").replace("%", "").replace(" ", "")
        == value
    )
    value = formatter.format(text_plan, "OTHER")
    assert value == text_plan


def test_large():
    validate(input_large, output_large_filtered)


def test_entity_type():
    entity_type_(reduce_, types)


def test_w2():
    validate(input_w2, output_w2)


def test_general():
    validate(input_entities, output_filtered)


def test_entity_type_w2():
    entity_type_(reduce_w2, types_w2)


def test_reduce_entities():
    entities_cleaned = preprocess.entity_cleaning(input_entities)
    entities_with_page_number = preprocess.replace_page_number(entities_cleaned, "9")
    mapped_entities = preprocess.map_entities(entities_with_page_number)
    reduced_entities = preprocess.reduce_entities(mapped_entities)
    assert reduced_entities == reduce_


def test_version_is_in_output():
    event = {"body": json.dumps({"entities": input_w2, "page": "10"})}
    response = json.loads(formatter.handler(event, context={})["body"])
    with open("tests/resources/generic-output.json", "w") as f:
        json.dump(response, f)
    assert "body" in response
    assert "version" in response
    assert response["version"].split("-")[:2] == ["2020", "10"]


def test_full_name_entities():
    with open("tests/resources/input_full_name_entities.json", "r") as o:
        inpt = json.load(o)
    with open("tests/resources/output_full_name_entities.json", "r") as o:
        otpt = json.load(o)
    response = formatter.add_full_name_entities(inpt)
    assert response == otpt


def test_no_person_name_input():
    with open("tests/resources/input_no_person_name.json", "r") as o:
        inpt = json.load(o)
    response = formatter.format_entities(inpt, "9")
    print("Got response: ", response)
    assert "PERSON NAME" not in response.keys()
    assert "FULL NAME" not in response.keys()


def test_address_split():
    red_address_out = preprocess.reduce_entities(address_map)
    assert red_address_out == address_out
