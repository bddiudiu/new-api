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

import React, { useEffect, useState, useContext } from 'react';
import { Button, Modal, Spin, Tag, Typography, Tooltip } from '@douyinfe/semi-ui';
import {
  IconCheckCircleStroked,
  IconClose,
  IconGift,
  IconCalendar,
  IconHistory,
} from '@douyinfe/semi-icons';
import { API, showError, showSuccess, renderQuota } from '../../../helpers';
import { UserContext } from '../../../context/User';

const { Title, Text } = Typography;

const SignInButton = ({ t }) => {
  const [userState, userDispatch] = useContext(UserContext);
  const [loading, setLoading] = useState(false);
  const [signLoading, setSignLoading] = useState(false);
  const [signInfo, setSignInfo] = useState(null);
  const [signList, setSignList] = useState([]);
  const [modalVisible, setModalVisible] = useState(false);

  // 获取签到信息
  const getSignInfo = async () => {
    setLoading(true);
    try {
      const res = await API.get('/api/user/sign/info');
      const { success, message, data } = res.data;
      if (success) {
        setSignInfo(data);
        setSignList(data.sign_list || []);
      } else {
        setSignInfo(null);
      }
    } catch (err) {
      console.error('获取签到信息失败:', err);
      setSignInfo(null);
    } finally {
      setLoading(false);
    }
  };

  // 执行签到
  const doSign = async () => {
    setSignLoading(true);
    try {
      const res = await API.post('/api/user/sign');
      const { success, message, data } = res.data;
      if (success) {
        showSuccess(t('签到成功') + '! ' + t('获得额度') + ': ' + renderQuota(data.quota));
        // 更新用户额度
        if (userState.user) {
          const updatedUser = {
            ...userState.user,
            quota: userState.user.quota + data.quota,
          };
          userDispatch({ type: 'login', payload: updatedUser });
        }
        // 刷新签到信息
        await getSignInfo();
      } else {
        showError(message);
      }
    } catch (err) {
      showError(t('签到失败'));
    } finally {
      setSignLoading(false);
    }
  };

  useEffect(() => {
    if (userState?.user?.id) {
      getSignInfo();
    }
  }, [userState?.user?.id]);

  // 如果用户未登录或签到功能未启用，不显示按钮
  if (!userState?.user?.id || (!loading && !signInfo)) {
    return null;
  }

  // 渲染签到日历
  const renderSignCalendar = () => {
    if (!signList || signList.length === 0) {
      return (
        <div className="text-center py-4">
          <Text type="tertiary">{t('暂无签到记录')}</Text>
        </div>
      );
    }

    return (
      <div className="grid grid-cols-7 gap-2 mt-4">
        {signList.map((item, index) => (
          <div
            key={index}
            className={`flex flex-col items-center p-2 rounded-lg ${
              item.signed
                ? 'bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800'
                : 'bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-700'
            }`}
          >
            <Text size="small" type="tertiary" className="text-xs">
              {item.date.split('-').slice(1).join('/')}
            </Text>
            {item.signed ? (
              <IconCheckCircleStroked
                style={{ color: 'var(--semi-color-success)', fontSize: 18 }}
              />
            ) : (
              <IconClose
                style={{ color: 'var(--semi-color-tertiary)', fontSize: 18 }}
              />
            )}
          </div>
        ))}
      </div>
    );
  };

  const buttonContent = signInfo?.signed_today ? (
    <Tooltip content={t('今日已签到')}>
      <Button
        icon={<IconCheckCircleStroked />}
        theme="light"
        type="tertiary"
        onClick={() => setModalVisible(true)}
      />
    </Tooltip>
  ) : (
    <Tooltip content={t('签到')}>
      <Button
        icon={<IconGift />}
        theme="solid"
        type="warning"
        onClick={() => setModalVisible(true)}
      />
    </Tooltip>
  );

  return (
    <>
      {buttonContent}

      <Modal
        title={
          <div className="flex items-center gap-2">
            <IconCalendar style={{ color: 'var(--semi-color-primary)' }} />
            <span>{t('签到')}</span>
          </div>
        }
        visible={modalVisible}
        onCancel={() => setModalVisible(false)}
        footer={null}
        width={500}
      >
        <Spin spinning={loading}>
          {signInfo && (
            <div className="space-y-4 pb-4">
              {/* 签到状态和按钮 */}
              <div className="flex items-center justify-between p-4 bg-gray-50 dark:bg-gray-800 rounded-lg">
                <div className="flex items-center gap-3">
                  <IconGift style={{ color: 'var(--semi-color-warning)', fontSize: 32 }} />
                  <div>
                    <Text strong>{t('签到奖励')}</Text>
                    <br />
                    <Text type="success" size="small">
                      {renderQuota(signInfo.quota_per_sign)} / {t('天')}
                    </Text>
                  </div>
                </div>

                <div>
                  {signInfo.signed_today ? (
                    <Tag color="green" size="large">
                      <IconCheckCircleStroked className="mr-1" />
                      {t('今日已签到')}
                    </Tag>
                  ) : signInfo.can_sign ? (
                    <Button
                      theme="solid"
                      type="warning"
                      size="large"
                      loading={signLoading}
                      onClick={doSign}
                      icon={<IconGift />}
                    >
                      {t('签到')}
                    </Button>
                  ) : (
                    <Tag color="grey" size="large">
                      {t('签到功能未启用')}
                    </Tag>
                  )}
                </div>
              </div>

              {/* 统计信息 */}
              <div className="grid grid-cols-2 gap-4">
                <div className="text-center p-4 bg-blue-50 dark:bg-blue-900/20 rounded-lg">
                  <Text size="small" type="tertiary">{t('累计签到天数')}</Text>
                  <Title heading={3} style={{ margin: '8px 0 0 0', color: 'var(--semi-color-primary)' }}>
                    {signInfo.total_sign_days || 0}
                  </Title>
                </div>
                <div className="text-center p-4 bg-orange-50 dark:bg-orange-900/20 rounded-lg">
                  <Text size="small" type="tertiary">{t('剩余可签到天数')}</Text>
                  <Title heading={3} style={{ margin: '8px 0 0 0', color: 'var(--semi-color-warning)' }}>
                    {signInfo.remaining_days >= 0 ? signInfo.remaining_days : 0}
                  </Title>
                </div>
              </div>

              {/* 签到日历 */}
              <div>
                <div className="flex items-center gap-2 mb-2">
                  <IconHistory style={{ color: 'var(--semi-color-tertiary)' }} />
                  <Text type="tertiary">{t('签到记录')}</Text>
                </div>
                {renderSignCalendar()}
              </div>

              {/* 签到说明 */}
              {signInfo.remaining_days <= 0 && (
                <div className="bg-yellow-50 dark:bg-yellow-900/20 p-3 rounded-lg">
                  <Text type="warning" size="small">
                    {t('签到功能未启用')}
                  </Text>
                </div>
              )}
            </div>
          )}
        </Spin>
      </Modal>
    </>
  );
};

export default SignInButton;
