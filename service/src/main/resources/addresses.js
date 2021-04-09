var addresses = angular.module('addresses', ['ngAnimate']);

addresses.controller('AddressesCtrl',
  function(
    $scope,
    $http,
    $filter,
    $httpParamSerializer,
    $log) {

  $scope.search = '';
  $scope.addresses = [];

  $scope.types = [
    {value: '104', selected: false, name: 'pilsēta'},
    {value: '113', selected: false, name: 'novads'},
    {value: '105', selected: false, name: 'pagasts'},
    {value: '106', selected: false, name: 'ciems'}];

  $scope.limit = {value: 20, selected: false, name: 'limits'};

  $scope.calculateUrlParams = function() {
    var types = [];
    angular.forEach($filter('filter')($scope.types, {selected: true}), function(value, key) {
      this.push(value.value);
    }, types);
    var limit = [];
    if ($scope.limit.selected) { limit.push($scope.limit.value); };
    return {search: $scope.search, type: types, limit: limit};
  };

  $scope.updateAddress = function () {
    var searched = $scope.search;
    var params = $scope.calculateUrlParams();
    $http.get('address', {params: params}).success(function(data) {
      if ($scope.search === searched) {
        $scope.addresses = data;
        $scope.urlParams = $httpParamSerializer(params);
      }
    });
  };

  $scope.addressClicked = function (a) {
    a.extended_view = ! a.extended_view;
  }

  $http.get('version').success(function(data) {
    $scope.normalizeVersion(data);
  });

  $scope.wsFailCount = 0;
  //get version updates through websocket
  function createWebsocket() {
    var protocol;
    if (window.location.protocol == "https:") protocol = "wss://"; else protocol = "ws://";
    let path = window.location.pathname;
    let base = path.substring(0, path.lastIndexOf("/"));
    let ws = new WebSocket(new URL("version-update", new URL(protocol + window.location.host + base + "/")));
    ws.onmessage = function(msg) {
      //call scope apply so scope is synchronized with view are executed
      $scope.$apply($scope.normalizeVersion(msg.data));
    };
    ws.onopen = function(evt) { $scope.wsFailCount = 0; $log.info(evt) };
    ws.onclose = function(evt) {
      if ($scope.wsFailCount < 5) createWebsocket();
      $log.info(evt);
    };
    ws.onerror = function(evt) { $scope.wsFailCount++; $log.error(evt) };
  };

  createWebsocket();

  $scope.normalizeVersion = function (version) {
    var i = version.indexOf('.');
    $scope.version = i == -1 ? version : version.substring(0, i);
  }

});
