import unittest

from core_prule.JsonRepository import JsonRepository
from core_prule.Configuration import Sources


class TestSources(unittest.TestCase):
    json_repo = JsonRepository()

    def test_deserialize(self):
        """
        Deserialize a JSON object into a Sources object
        """

        json_obj = self.json_repo.load_str("""
            {
              id: "a",
              name: "b",
              description: "c",
            
              sources: [
              ]
            }        
        """)
        result = Sources.from_dict(json_obj)

        self.assertEqual(result.key, 'a')
        self.assertEqual(result.name, 'b')
        self.assertEqual(result.description, 'c')
        self.assertEqual(result.sources, [])

    def test_deserialize_nested(self):
        """
        Deserialize to table configuration
        """

        json_obj = self.json_repo.load_str("""
            {
              id: "a",
              name: "b",
              description: "c",

              sources: [
                  {
                      id: "d",
                      name: "e",
                      description: "f",
                      path: "g",
                      type: "h",
                      table: {
                            name: "i",
                            description: "j",
                            deduplicate: true,
                            trim: true,
                            columns: []
                      }
                  }
              ]
            }        
        """)
        result = Sources.from_dict(json_obj)

        self.assertEqual('a', result.key)
        self.assertEqual('b', result.name)
        self.assertEqual('c', result.description)
        self.assertEqual(1, len(result.sources))
        source_definition = result.sources.__getitem__(0)
        self.assertEqual('d', source_definition.key)
        self.assertEqual('e', source_definition.name)
        self.assertEqual('f', source_definition.description)
        self.assertEqual('g', source_definition.path)
        self.assertEqual('h', source_definition.type)
        self.assertEqual(',', source_definition.table.delimiter_or_default())

    def test_deserialize_config_file(self):
        """
        Deserialize a sample config file
        """

        json_obj = TestSources.json_repo.load_file('./config/sample1/sample1.tables.json5')
        result = Sources.from_dict(json_obj)

        self.assertIsNotNone(result)


if __name__ == '__main__':
    unittest.main()
