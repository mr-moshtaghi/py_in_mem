import time
from unittest import TestCase
from py_mem import PyInMemStore


class PyInMemStoreTestCase(TestCase):

    def setUp(self):
        self.data_store = PyInMemStore()
        self.data_store.set(key="city", value="tehran")
        self.data_store.set(key="language", value="persian")

    def test_get(self):
        self.assertEqual(self.data_store.get("city"), "tehran")
        self.assertEqual(self.data_store.get("language"), "persian")

        self.assertIsNone(self.data_store.get("father"))

    def test_set(self):
        self.data_store.set(key="age", value=26)
        self.data_store.set(key="name", value="sajjad")
        self.assertEqual(self.data_store.get("name"), "sajjad")
        self.assertEqual(self.data_store.get("age"), 26)

    def test_delete(self):
        self.data_store.delete(key="city")
        self.assertIsNone(self.data_store.get("city"))

        with self.assertRaises(KeyError) as e:
            self.data_store.delete("father")

    def test_expire(self):
        with self.assertRaises(ValueError):
            self.data_store.expire(key="city", seconds=-4)

        with self.assertRaises(KeyError):
            self.data_store.expire(key="father", seconds=4)

        self.data_store.expire(key="city", seconds=1)
        self.assertIsNotNone(self.data_store.get("city"))
        time.sleep(2)
        self.assertIsNone(self.data_store.get("city"))

    def test_expire_and_delete_me(self):
        self.data_store.expire(key="city", seconds=1)
        self.assertIsNotNone(self.data_store.get("city"))
        self.data_store.delete(key="city")

    def test_persist_and_load(self):
        self.data_store.persist(filename="test_persist.txt")
        new_data_stor = PyInMemStore()
        self.assertIsNone(new_data_stor.get("city"))
        new_data_stor.load(filename="test_persist.txt")
        self.assertEqual(new_data_stor.get("city"), "tehran")

    def test_transaction_commit(self):
        self.data_store.begin()
        self.data_store.set("father", "mohamad")
        self.data_store.delete("city")
        self.assertEqual(self.data_store.in_transaction, True)
        self.data_store.commit()

        self.assertEqual(self.data_store.in_transaction, False)

        self.assertEqual(self.data_store.get("father"), "mohamad")
        self.assertIsNone(self.data_store.get("city"))

    def test_transaction_rollback(self):
        self.data_store.begin()
        self.data_store.set("father", "mohamad")
        self.data_store.delete("city")
        self.assertEqual(self.data_store.in_transaction, True)
        self.data_store.rollback()

        self.assertEqual(self.data_store.in_transaction, False)

        self.assertIsNone(self.data_store.get("father"))
        self.assertEqual(self.data_store.get("city"), "tehran")






