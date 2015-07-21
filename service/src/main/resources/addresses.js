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
    {value: '106', selected: false, name: 'ciems'},
    {value: '107', selected: false, name: 'iela'},
    {value: '108', selected: false, name: 'māja'},
    {value: '109', selected: false, name: 'dzīvoklis'}];

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

  //get version updates through websocket
  function createWebsocket() {
    var ws = new WebSocket("ws://" + window.location.host + "/version-update");
    ws.onmessage = function(msg) {
      //call scope apply so scope is synchronized with view are executed
      $scope.$apply($scope.normalizeVersion(msg.data));
    };
    ws.onopen = function(evt) { $log.info(evt) };
    ws.onclose = function(evt) {
      createWebsocket();
      $log.info(evt);
    };
    ws.onerror = function(evt) { $log.error(evt) };
  };

  createWebsocket();

  $scope.normalizeVersion = function (version) {
    var i = version.indexOf('.');
    $scope.version = i == -1 ? version : version.substring(0, i);
  }

});
