import taskclustercoalesce.coalescer as coalescer
from mockredis import mock_redis_client
import unittest
import mock


class CoalescerTestBase(unittest.TestCase):

    def setUp(self):
        self.prefix = u'testing.prefix.'
        self.m_redis = mock_redis_client()
        self.m_stats = mock.Mock()
        self.coalescer = coalescer.CoalescingMachine(self.prefix,
                                                     self.m_redis,
                                                     self.m_stats)

    def tearDown(self):
        pass


class CoalescerTest(CoalescerTestBase):

    def test_insert_task_single_taskid_list(self):
        taskId = 'taskId1'
        self.coalescer.insert_task(taskId, 'key')
        actual_list_members = self.m_redis.lrange(
                self.prefix + 'lists.' + 'key', 0, -1)
        expected_list_members = [taskId]
        self.assertEqual(actual_list_members, expected_list_members)

    def test_insert_task_multi_taskid_list(self):
        taskId1, taskId2, taskId3 = 'taskId1', 'taskId2', 'taskId3'
        self.coalescer.insert_task(taskId1, 'key')
        self.coalescer.insert_task(taskId2, 'key')
        self.coalescer.insert_task(taskId3, 'key')
        actual_list_members = self.m_redis.lrange(
                self.prefix + 'lists.' + 'key', 0, -1)
        expected_list_members = [taskId3, taskId2, taskId1]
        self.assertEqual(actual_list_members, expected_list_members)

    def test_insert_task_single_key_set(self):
        key = 'sample_key1'
        self.coalescer.insert_task('taskId1', key)
        actual_set_members = self.m_redis.smembers(self.prefix + 'list_keys')
        expected_set_members = set([key])
        self.assertEqual(actual_set_members, expected_set_members)

    def test_insert_task_multi_key_set(self):
        key1, key2, key3 = 'sample_key1', 'sample_key2', 'sample_key3'
        self.coalescer.insert_task('taskId1', key1)
        self.coalescer.insert_task('taskId1', key2)
        self.coalescer.insert_task('taskId1', key3)
        actual_set_members = self.m_redis.smembers(self.prefix + 'list_keys')
        expected_set_members = set([key1, key2, key3])
        self.assertEqual(actual_set_members, expected_set_members)

    def test_insert_task_single_key_set_exists(self):
        prefix = self.prefix + 'list_keys'
        key = 'sample_key1'
        self.m_redis.sadd(prefix, key)
        self.coalescer.insert_task('taskId1', key)
        actual_set_members = self.m_redis.smembers(prefix)
        expected_set_members = set([key])
        self.assertEqual(actual_set_members, expected_set_members)

    def test_remove_task_single_taskid_list(self):
        prefix = self.prefix + 'lists.' + 'key'
        taskId = 'taskId'
        self.m_redis.lpush(prefix, taskId)
        self.coalescer.remove_task(taskId, 'key')
        actual_list_members = self.m_redis.lrange(prefix, 0, -1)
        expected_list_members = []
        self.assertEqual(actual_list_members, expected_list_members)

    def test_remove_task_multi_taskid_list(self):
        prefix = self.prefix + 'lists.' + 'key'
        taskId1, taskId2, taskId3 = 'taskId1', 'taskId2', 'taskId3'
        self.m_redis.lpush(prefix, taskId1, taskId2, taskId3)
        self.coalescer.remove_task(taskId2, 'key')
        actual_list_members = self.m_redis.lrange(prefix, 0, -1)
        expected_list_members = [taskId3, taskId1]
        self.assertEqual(actual_list_members, expected_list_members)

    def test_remove_task_single_key_set(self):
        prefix = self.prefix + 'list_keys'
        key = 'sample_key1'
        self.m_redis.sadd(prefix, key)
        self.coalescer.remove_task('taskId1', key)
        actual_set_members = self.m_redis.smembers(prefix)
        expected_set_members = set([])
        self.assertEqual(actual_set_members, expected_set_members)

    def test_remove_task_multi_key_set(self):
        prefix = self.prefix + 'list_keys'
        key1, key2, key3 = 'sample_key1', 'sample_key2', 'sample_key3'
        self.m_redis.sadd(prefix, key1, key2, key3)
        self.coalescer.remove_task('taskId1', key2)
        actual_set_members = self.m_redis.smembers(prefix)
        expected_set_members = set([key1, key3])
        self.assertEqual(actual_set_members, expected_set_members)

    def test_remove_task_nonexistent_key(self):
        prefix = self.prefix + 'lists.' + 'key'
        self.coalescer.remove_task('taskId', 'key')
        actual_list_members = self.m_redis.lrange(prefix, 0, -1)
        expected_list_members = []
        self.assertEqual(actual_list_members, expected_list_members)

    def test_remove_task_nonexistent_taskid(self):
        prefix = self.prefix + 'list_keys'
        self.coalescer.remove_task('taskId', 'sample_key')
        actual_set_members = self.m_redis.smembers(prefix)
        expected_set_members = set([])
        self.assertEqual(actual_set_members, expected_set_members)
