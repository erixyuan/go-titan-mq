<template>
  <div class="container">
    <Breadcrumb :items="['消息页', '消息列表']" />
    <a-card class="general-card" title="消息列表">
      <a-row>
        <a-col :flex="1">
          <a-form
            :model="formModel"
            :label-col-props="{ span: 4 }"
            :wrapper-col-props="{ span: 18 }"
            label-align="left"
          >
            <a-row :gutter="16">
              <a-col :span="8">
                <a-form-item field="topic" label="消息主题">
                  <a-select
                    v-model="formModel.topic"
                    :options="filterTopicOptions"
                    placeholder="请选择主题"
                    @change="selectTopicChange"
                  />
                </a-form-item>
              </a-col>
              <a-col :span="8">
                <a-form-item field="queueId" label="消费队列">
                  <a-select
                    v-model="formModel.queueId"
                    :options="queueIdOptions"
                    placeholder="消费队列编号"
                  />
                </a-form-item>
              </a-col>
            </a-row>
          </a-form>
        </a-col>
        <a-divider style="height: 84px" direction="vertical" />
        <a-col :flex="'86px'" style="text-align: right">
          <a-space direction="vertical" :size="18">
            <a-button type="primary" @click="search"
              ><template #icon><icon-search /></template>搜索</a-button
            >
            <a-button @click="reset"
              ><template #icon><icon-refresh /></template>重置</a-button
            >
          </a-space>
        </a-col>
      </a-row>
      <a-divider style="margin-top: 0" />
      <a-row style="margin-bottom: 16px">
        <a-col :span="12">
          <a-space>
            <a-button type="primary">
              <template #icon>
                <icon-plus />
              </template>
              创建
            </a-button>
            <a-upload action="/">
              <template #upload-button>
                <a-button>导入</a-button>
              </template>
            </a-upload>
          </a-space>
        </a-col>
        <a-col
          :span="12"
          style="display: flex; align-items: center; justify-content: end"
        >
          <a-button>
            <template #icon>
              <icon-download />
            </template>
            下载
          </a-button>
          <a-tooltip content="刷新">
            <div class="action-icon" @click="search"
              ><icon-refresh size="18"
            /></div>
          </a-tooltip>
        </a-col>
      </a-row>
      <a-table
        row-key="id"
        :loading="loading"
        :pagination="pagination"
        :columns="(cloneColumns as TableColumnData[])"
        :data="renderData"
        :bordered="false"
        :size="size"
        @page-change="onPageChange"
      >
      </a-table>
    </a-card>
  </div>
</template>

<script lang="ts" setup>
  import { computed, ref, reactive, watch, nextTick } from 'vue';
  import { useI18n } from 'vue-i18n';
  import useLoading from '@/hooks/loading';
  import { Pagination } from '@/types/global';
  import type { SelectOptionData } from '@arco-design/web-vue/es/select/interface';
  import type { TableColumnData } from '@arco-design/web-vue/es/table/interface';
  import cloneDeep from 'lodash/cloneDeep';
  import Sortable from 'sortablejs';
  import {
    MessageParams,
    MessageRecord,
    queryMessageList,
  } from '@/api/message';
  import { queryTopicList, TopicRecord } from '@/api/topic';

  type SizeProps = 'mini' | 'small' | 'medium' | 'large';
  type Column = TableColumnData & { checked?: true };

  const generateFormModel = () => {
    return {
      queueId: 0,
      topic: '',
    };
  };
  const { loading, setLoading } = useLoading(true);
  const { t } = useI18n();
  const renderData = ref<MessageRecord[]>([]);
  const formModel = ref(generateFormModel());
  const cloneColumns = ref<Column[]>([]);
  const showColumns = ref<Column[]>([]);

  const size = ref<SizeProps>('medium');

  const basePagination: Pagination = {
    current: 1,
    pageSize: 20,
    total: 20,
  };
  const pagination = reactive({
    ...basePagination,
    showTotal: true,
  });
  const densityList = computed(() => [
    {
      name: t('mqTable.size.mini'),
      value: 'mini',
    },
    {
      name: t('mqTable.size.small'),
      value: 'small',
    },
    {
      name: t('mqTable.size.medium'),
      value: 'medium',
    },
    {
      name: t('mqTable.size.large'),
      value: 'large',
    },
  ]);
  const columns = computed<TableColumnData[]>(() => [
    {
      title: '逻辑偏移',
      render: ({ record }) => {
        if (record === undefined) {
          return 0;
        }
        return record.offset;
      },
    },
    {
      title: '消息ID',
      dataIndex: 'messageId',
    },
    {
      title: 'HashCode',
      dataIndex: 'tagHashCode',
    },
    {
      title: '大小',
      dataIndex: 'size',
      slotName: 'size',
    },
    {
      title: '物理偏移',
      dataIndex: 'commitLogOffset',
    },
  ]);

  // 主题选择下拉
  let topicList = ref<TopicRecord[]>([]);
  const filterTopicOptions = ref<SelectOptionData[]>([]);
  const queueIdOptions = ref<SelectOptionData[]>([]);
  const fetchTopicData = async () => {
    setLoading(true);
    try {
      const { data } = await queryTopicList();
      topicList = ref(data.list);
      filterTopicOptions.value = data.list.map((item: any) => ({
        label: item.name,
        value: item.name,
      }));
      // 初始化下拉选项中的第一个
      const [first] = data.list;
      selectTopic(first);
    } finally {
      setLoading(false);
    }
  };
  function selectTopic(t: TopicRecord) {
    formModel.value.topic = t.name;
    if (t.queueIds != null && t.queueIds.length > 0) {
      queueIdOptions.value = t.queueIds.map((item: number) => ({
        label: `${item}队列`,
        value: item,
      }));
    } else {
      queueIdOptions.value = [];
    }
  }
  function selectTopicChange() {
    topicList.value?.forEach(function (el) {
      if (el.name === formModel.value.topic) {
        selectTopic(el);
      }
    });
  }
  fetchTopicData();

  // 拉取数据
  const fetchData = async (
    params: MessageParams = {
      current: 1,
      pageSize: 20,
      topic: formModel.value.topic,
      queueId: formModel.value.queueId,
    }
  ) => {
    setLoading(true);
    try {
      const { data } = await queryMessageList(params);
      renderData.value = data.list;
      pagination.current = params.current;
      pagination.total = data.total;
    } catch (err) {
      alert(err);
    } finally {
      setLoading(false);
    }
  };

  // 搜索入口
  const search = () => {
    fetchData({
      ...basePagination,
      ...formModel.value,
    } as unknown as MessageParams);
  };
  // 分页点击入口
  const onPageChange = (current: number) => {
    fetchData({ ...basePagination, current, ...formModel.value });
  };

  const reset = () => {
    formModel.value = generateFormModel();
  };

  watch(
    () => columns.value,
    (val) => {
      cloneColumns.value = cloneDeep(val);
      cloneColumns.value.forEach((item, index) => {
        item.checked = true;
      });
      showColumns.value = cloneDeep(cloneColumns.value);
    },
    { deep: true, immediate: true }
  );
</script>

<script lang="ts">
  export default {
    name: 'Message',
  };
</script>

<style scoped lang="less">
  .container {
    padding: 0 20px 20px 20px;
  }
  :deep(.arco-table-th) {
    &:last-child {
      .arco-table-th-item-title {
        margin-left: 16px;
      }
    }
  }
  .action-icon {
    margin-left: 12px;
    cursor: pointer;
  }
  .active {
    color: #0960bd;
    background-color: #e3f4fc;
  }
  .setting {
    display: flex;
    align-items: center;
    width: 200px;
    .title {
      margin-left: 12px;
      cursor: pointer;
    }
  }
</style>
