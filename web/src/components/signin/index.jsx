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
import {
  Card,
  Button,
  Typography,
  Spin,
  Tag,
  Toast,
} from '@douyinfe/semi-ui';
import {
  IconCheckCircleStroked,
  IconClose,
  IconGift,
  IconCalendar,
} from '@douyinfe/semi-icons';
import { useTranslation } from 'react-i18next';
import { API, showError, showSuccess, renderQuota } from '../../helpers';
import { UserContext } from '../../context/User';

const { Title, Text } = Typography;

const SignIn = () => {
  const { t } = useTranslation();
  const [userState, userDispatch] = useContext(UserContext);

  const [loading, setLoading] = useState(false);
  const [signLoading, setSignLoading] = useState(false);
  const [signInfo, setSignInfo] = useState(null);
  const [signList, setSignList] = useState([]);

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
        // 签到功能未启用或不符合条件时不显示错误
        if (message !== '签到功能未启用' && message !== 'Sign-in feature is not enabled') {
          showError(message);
        }
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
    getSignInfo();
  }, []);

  // 如果签到功能未启用，不显示任何内容
  if (!loading && !signInfo) {
    return null;
  }

  // 渲染日历格式的签到记录
  const renderSignCalendar = () => {
    if (!signList || signList.length === 0) {
      return (
        <div className="text-center py-4 text-gray-500">
          <Text type="tertiary">{t('暂无签到记录')}</Text>
        </div>
      );
    }

    // 按日期分组显示最近7天的签到状态
    const recentDays = signList.slice(-7);

    return (
      <div className="flex flex-wrap gap-2 justify-center mt-4">
        {recentDays.map((item, index) => (
          <div
            key={index}
            className={`flex flex-col items-center p-2 rounded-lg ${
              item.signed
                ? 'bg-green-50 dark:bg-green-900/20'
                : 'bg-gray-50 dark:bg-gray-800'
            }`}
            style={{ minWidth: '60px' }}
          >
            <Text size="small" type="tertiary">
              {item.date.split('-').slice(1).join('/')}
            </Text>
            {item.signed ? (
              <IconCheckCircleStroked
                style={{ color: 'var(--semi-color-success)', fontSize: 20 }}
              />
            ) : (
              <IconClose
                style={{ color: 'var(--semi-color-tertiary)', fontSize: 20 }}
              />
            )}
          </div>
        ))}
      </div>
    );
  };

  return (
    <Card
      className="w-full"
      title={
        <div className="flex items-center gap-2">
          <IconCalendar style={{ color: 'var(--semi-color-primary)' }} />
          <span>{t('签到')}</span>
        </div>
      }
    >
      <Spin spinning={loading}>
        {signInfo && (
          <div className="space-y-4">
            {/* 签到状态 */}
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <IconGift style={{ color: 'var(--semi-color-warning)', fontSize: 24 }} />
                <div>
                  <Text strong>{t('签到奖励')}</Text>
                  <br />
                  <Text type="tertiary" size="small">
                    {renderQuota(signInfo.quota_per_sign)} / {t('天')}
                  </Text>
                </div>
              </div>

              <div className="text-right">
                {signInfo.signed_today ? (
                  <Tag color="green" size="large">
                    {t('今日已签到')}
                  </Tag>
                ) : signInfo.can_sign ? (
                  <Button
                    theme="solid"
                    type="primary"
                    loading={signLoading}
                    onClick={doSign}
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
            <div className="grid grid-cols-2 gap-4 py-4 border-t border-b border-gray-100 dark:border-gray-700">
              <div className="text-center">
                <Text size="small" type="tertiary">{t('累计签到天数')}</Text>
                <Title heading={4} style={{ margin: 0 }}>
                  {signInfo.total_sign_days || 0}
                </Title>
              </div>
              <div className="text-center">
                <Text size="small" type="tertiary">{t('剩余可签到天数')}</Text>
                <Title heading={4} style={{ margin: 0 }}>
                  {signInfo.remaining_days >= 0 ? signInfo.remaining_days : 0}
                </Title>
              </div>
            </div>

            {/* 签到日历 */}
            <div>
              <Text type="tertiary" size="small">{t('签到记录')} ({t('最近7天')})</Text>
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
    </Card>
  );
};

export default SignIn;
