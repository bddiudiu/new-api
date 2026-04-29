/*
Copyright (C) 2025 QuantumNous

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <https://www.gnu.org/licenses/>.

For commercial licensing, please contact support@quantumnous.com
*/

import React, { useEffect, useState } from 'react';
import {
  Banner,
  Button,
  Card,
  Form,
  Input,
  InputNumber,
  Modal,
  Space,
  Table,
  DatePicker,
  Typography,
} from '@douyinfe/semi-ui';
import { IconDelete, IconPlus } from '@douyinfe/semi-icons';
import { API, showError, showSuccess, showWarning } from '../../../helpers';
import { useTranslation } from 'react-i18next';

const { Text } = Typography;

function formatLocalDate(date) {
  const value = new Date(date);
  const year = value.getFullYear();
  const month = String(value.getMonth() + 1).padStart(2, '0');
  const day = String(value.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

export default function SettingsQuotaExpiry({ options = {}, refresh }) {
  const { t } = useTranslation();
  const [loading, setLoading] = useState(false);
  const [rules, setRules] = useState([]);
  const [addModalVisible, setAddModalVisible] = useState(false);
  const [rebuildModalVisible, setRebuildModalVisible] = useState(false);
  const [rebuildDate, setRebuildDate] = useState(null);
  const [rebuildLoading, setRebuildLoading] = useState(false);

  const [newRule, setNewRule] = useState({
    label: '',
    log_type: 0,
    expire_days: 30,
  });

  useEffect(() => {
    setLoading(true);
    try {
      const rawRules = options['quota_expiry_setting.rules'];
      setRules(rawRules ? JSON.parse(rawRules) : []);
    } catch {
      setRules([]);
      showError(t('加载配置失败'));
    } finally {
      setLoading(false);
    }
  }, [options, t]);

  const saveSettings = async (newRules) => {
    try {
      const res = await API.put('/api/option/', {
        key: 'quota_expiry_setting.rules',
        value: JSON.stringify(newRules),
      });
      if (res.data.success) {
        showSuccess(t('保存成功'));
        setRules(newRules);
        if (typeof refresh === 'function') {
          await refresh();
        }
      } else {
        showError(res.data.message || t('保存失败'));
      }
    } catch {
      showError(t('保存失败'));
    }
  };

  const handleAddRule = () => {
    if (!newRule.label || newRule.log_type === 0 || newRule.expire_days <= 0) {
      showWarning(t('请填写完整信息'));
      return;
    }
    const updatedRules = [...rules, newRule];
    saveSettings(updatedRules);
    setAddModalVisible(false);
    setNewRule({ label: '', log_type: 0, expire_days: 30 });
  };

  const handleDeleteRule = (index) => {
    const updatedRules = rules.filter((_, i) => i !== index);
    saveSettings(updatedRules);
  };

  const handleRebuild = async () => {
    if (!rebuildDate) {
      showWarning(t('请选择开始日期'));
      return;
    }

    setRebuildLoading(true);
    try {
      const dateStr = formatLocalDate(rebuildDate);
      const res = await API.post('/api/quota-expiry/rebuild', {
        start_date: dateStr,
      });

      if (res.data.success) {
        showSuccess(t('重建任务已启动'));
        pollRebuildStatus(res.data.data.task_id);
      } else {
        showError(res.data.message || t('启动重建任务失败'));
        setRebuildLoading(false);
      }
    } catch {
      showError(t('启动重建任务失败'));
      setRebuildLoading(false);
    }
  };

  const pollRebuildStatus = async (taskId) => {
    const interval = setInterval(async () => {
      try {
        const res = await API.get(`/api/quota-expiry/rebuild/status?task_id=${taskId}`);
        if (res.data.success) {
          if (res.data.data.status === 'completed') {
            clearInterval(interval);
            setRebuildLoading(false);
            setRebuildModalVisible(false);
            showSuccess(t('重建完成'));
          } else if (res.data.data.status === 'failed') {
            clearInterval(interval);
            setRebuildLoading(false);
            showError(t('重建失败') + ': ' + res.data.data.error);
          }
        }
      } catch {
        clearInterval(interval);
        setRebuildLoading(false);
        showError(t('查询重建状态失败'));
      }
    }, 2000);
  };

  const columns = [
    {
      title: t('日志类型名称'),
      dataIndex: 'label',
      key: 'label',
    },
    {
      title: t('日志类型值'),
      dataIndex: 'log_type',
      key: 'log_type',
    },
    {
      title: t('有效期(天)'),
      dataIndex: 'expire_days',
      key: 'expire_days',
    },
    {
      title: t('操作'),
      key: 'action',
      render: (text, record, index) => (
        <Button
          type="danger"
          icon={<IconDelete />}
          onClick={() => handleDeleteRule(index)}
        >
          {t('删除')}
        </Button>
      ),
    },
  ];

  return (
    <div>
      <Banner
        type="info"
        description={t('配置特定日志类型的额度有效期。到期后，未使用的额度将被作废。')}
      />
      <Card
        title={t('额度有效期规则')}
        headerExtraContent={
          <Space>
            <Button
              icon={<IconPlus />}
              onClick={() => setAddModalVisible(true)}
            >
              {t('新增规则')}
            </Button>
            <Button
              type="tertiary"
              onClick={() => setRebuildModalVisible(true)}
            >
              {t('初始化重建')}
            </Button>
          </Space>
        }
        loading={loading}
      >
        <Table
          columns={columns}
          dataSource={rules}
          pagination={false}
          empty={t('暂无规则')}
        />
      </Card>

      <Modal
        title={t('新增有效期规则')}
        visible={addModalVisible}
        onOk={handleAddRule}
        onCancel={() => setAddModalVisible(false)}
      >
        <Form>
          <Form.Input
            field="label"
            label={t('日志类型名称')}
            placeholder={t('例如：活动')}
            value={newRule.label}
            onChange={(value) => setNewRule({ ...newRule, label: value })}
          />
          <Form.InputNumber
            field="log_type"
            label={t('日志类型值')}
            placeholder={t('例如：4')}
            value={newRule.log_type}
            onChange={(value) => setNewRule({ ...newRule, log_type: value })}
          />
          <Form.InputNumber
            field="expire_days"
            label={t('有效期(天)')}
            value={newRule.expire_days}
            onChange={(value) => setNewRule({ ...newRule, expire_days: value })}
            min={1}
          />
        </Form>
      </Modal>

      <Modal
        title={t('初始化重建额度有效期数据')}
        visible={rebuildModalVisible}
        onOk={handleRebuild}
        onCancel={() => setRebuildModalVisible(false)}
        confirmLoading={rebuildLoading}
      >
        <Space vertical align="start">
          <Text>{t('选择开始日期，将从该日期起重建所有符合规则的额度有效期记录')}</Text>
          <DatePicker
            value={rebuildDate}
            onChange={setRebuildDate}
            placeholder={t('选择开始日期')}
          />
          {rebuildLoading && <Text type="warning">{t('重建中，请稍候...')}</Text>}
        </Space>
      </Modal>
    </div>
  );
}
