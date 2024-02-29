import hail as hl

data = [
    {
        "k1": "key1",
        "k2": "key2",
        "a": 2,
        "b": "text",
        "c": True,
        "d": 1234567890123456789,
        "e": 2.71828,
        "f": 3.141592653589793,
        "g": hl.call(1, 0),  # Genotype call
        "h": hl.locus("chr1", 10000, reference_genome="GRCh38"),  # Locus
        "i": [1, 2, 3],
        "complex_dictionary": {"key1": 5, "key2": 10},
        "k": {11, 12, 13},  # Set
        "detailed_struct": {
            "long_field1": 14,
            "long_field2": "text",
        },  # Struct with longer field names
        "array_of_structs": [
            {
                "long_n": 15,
                "long_o": "text1",
                "inner_array_of_structs": [
                    {
                        "inner_n": None,
                        "inner_o": "inner_text1",
                        "inner_s": {"another_field": "value1"},
                    },
                    {
                        "inner_n": 2,
                        "inner_o": "inner_text2",
                        "inner_s": {"another_field": "value2"},
                    },
                ],
            },
            {
                "long_n": 16,
                "long_o": "text2",
                "inner_array_of_structs": [],
            },
        ],  # Array of structs with another array of structs inside it
        "nested_complex_struct": {
            "q": 17,
            "detailed_struct": {
                "long_field1": 14,
                "long_field2": "text",
            },  # Additional nested struct with same schema and name as "detailed_struct"
            "inner_struct": {"long_s": 18, "long_t": "text3"},
        },  # Nested struct with longer field names
        "optional_field": 19,  # Additional field for testing missing data
        "deeply_nested_struct": {  # Deeply nested struct
            "outer_field": {"inner_field1": 20, "inner_field2": None}
        },
    },
    {
        "k1": "key3",
        "k2": "key4",
        "a": 5,
        "b": "more_text",
        "c": False,
        "d": 98765432109876,
        "e": 3.14159,
        "f": 1.618033988749895,
        "g": hl.call(0, 1),  # Genotype call
        "h": hl.locus("chr2", 20000, reference_genome="GRCh38"),  # Locus
        "i": [4, 5, 6],
        "complex_dictionary": {"key3": 15, "key4": None},
        "k": {14, 15, 16},  # Set
        "detailed_struct": {
            "long_field1": None,  # missing data
            "long_field2": "more_text",
        },  # Struct with longer field names
        "array_of_structs": [
            {
                "long_n": 25,
                "long_o": "text4",
                "inner_array_of_structs": [
                    {
                        "inner_n": 5,
                        "inner_o": "inner_text5",
                        "inner_s": {"another_field": None},
                    },
                    {
                        "inner_n": 6,
                        "inner_o": "inner_text6",
                        "inner_s": {"another_field": "value6"},
                    },
                ],
            },
            {
                "long_n": None,
                "long_o": "text5",
                "inner_array_of_structs": [
                    {
                        "inner_n": 7,
                        "inner_o": "inner_text7",
                        "inner_s": {"another_field": "value7"},
                    },
                    {
                        "inner_n": 8,
                        "inner_o": "inner_text8",
                        "inner_s": {"another_field": "value8"},
                    },
                ],
            },
        ],  # Array of structs with another array of structs inside it
        "nested_complex_struct": {
            "q": 27,
            "detailed_struct": {
                "long_field1": None,  # missing data
                "long_field2": "more_text",
            },  # Additional nested struct with same schema and name as "detailed_struct"
            "inner_struct": {"long_s": 28, "long_t": None},  # long_t missing
        },  # Nested struct with longer field names
        "optional_field": None,  # Additional field for testing missing data
        "deeply_nested_struct": None,  # Entire nested struct missing
    },
    # Additional rows can be added here
]

data_type = (
    "array<struct{"
    "k1: str, k2: str, "
    "a: int32, b: str, c: bool, d: int64, e: float32, f: float64, "
    "g: call, h: locus<GRCh38>, i: array<int32>, "
    "complex_dictionary: dict<str, int32>, k: set<int32>, "
    "detailed_struct: struct{long_field1: int32, long_field2: str}, "
    "array_of_structs: array<struct{"
    "long_n: int32, long_o: str, "
    "inner_array_of_structs: array<struct{inner_n: int32, inner_o: str, inner_s: struct{another_field: str}}>"
    "}>,"
    "nested_complex_struct: struct{"
    "q: int32, "
    "detailed_struct: struct{long_field1: int32, long_field2: str}, "
    "inner_struct: struct{long_s: int32, long_t: str}"
    "}, "
    "optional_field: int32, "
    "deeply_nested_struct: struct{outer_field: struct{inner_field1: int32, inner_field2: str}}"
    "}>"
)

ht = hl.Table.parallelize(hl.literal(data, data_type), key=["k1", "k2"])
