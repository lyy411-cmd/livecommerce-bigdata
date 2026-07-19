// popup.js - 扩展弹窗脚本
document.addEventListener('DOMContentLoaded', () => {
  const $sub = document.getElementById('sub');
  const $status = document.getElementById('captureStatus');
  const $room = document.getElementById('roomId');
  const $captured = document.getElementById('captured');
  const $posted = document.getElementById('posted');
  const $backend = document.getElementById('backend');
  const $toggle = document.getElementById('toggleBtn');
  const $open = document.getElementById('openRoomBtn');

  let currentEnabled = true;

  function renderDot(enabled, label) {
    return `<span class="dot ${enabled ? 'on' : 'off'}"></span>${label}`;
  }

  function refresh() {
    chrome.runtime.sendMessage({ type: 'QUERY_ACTIVE' }, (status) => {
      if (!status) {
        $sub.textContent = '当前页面不是抖音直播间';
        $status.innerHTML = renderDot(false, '未激活');
        $room.textContent = '-';
        $captured.textContent = '0';
        $posted.textContent = '0';
        $backend.innerHTML = renderDot(false, '未连接');
        return;
      }

      const onDouyin = (status.url || '').indexOf('live.douyin.com') !== -1;
      $sub.textContent = onDouyin ? '已连接到抖音直播间' : '请打开抖音直播间';

      $status.innerHTML = renderDot(status.enabled && status.connected,
        status.connected ? '采集中' : (status.enabled ? '等待连接' : '已暂停'));

      $room.textContent = status.currentRoomId || '-';
      $captured.textContent = String(status.totalCaptured || 0);
      $posted.textContent = String(status.totalPosted || 0);
      $backend.innerHTML = renderDot(status.backendReachable,
        status.backendReachable ? '已连接' : '未连接');

      currentEnabled = !!status.enabled;
      $toggle.textContent = currentEnabled ? '暂停捕获' : '启用捕获';
    });
  }

  $toggle.addEventListener('click', () => {
    chrome.tabs.query({ active: true, currentWindow: true }, (tabs) => {
      if (!tabs || !tabs[0]) return;
      chrome.tabs.sendMessage(tabs[0].id, {
        type: 'TOGGLE',
        enabled: !currentEnabled,
      }, () => {
        currentEnabled = !currentEnabled;
        $toggle.textContent = currentEnabled ? '暂停捕获' : '启用捕获';
        setTimeout(refresh, 300);
      });
    });
  });

  $open.addEventListener('click', () => {
    chrome.tabs.create({ url: 'https://live.douyin.com/' });
  });

  refresh();
  // 每 1.5 秒自动刷新
  setInterval(refresh, 1500);
});
