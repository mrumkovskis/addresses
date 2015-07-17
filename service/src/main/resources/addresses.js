var addresses = angular.module('addresses', []);

addresses.controller('AddressesCtrl', function($scope, $http, $filter,$httpParamSerializer) {
  $scope.search = '';
  $scope.addresses = [];

  $scope.types = [
    { name: '104', selected: false, nosaukums: 'pilsēta' },
    { name: '113', selected: false, nosaukums: 'novads' },
    { name: '105', selected: false, nosaukums: 'pagasts' },
    { name: '106', selected: false, nosaukums: 'ciems' },
    { name: '107', selected: false, nosaukums: 'iela' },
    { name: '108', selected: false, nosaukums: 'māja' },
    { name: '109', selected: false, nosaukums: 'dzīvoklis' }];

  $scope.calculateUrlParams = function() {
    $scope.selectedTypes = [];
    angular.forEach($filter('filter')($scope.types, {selected: true}), function(value, key) {
      this.push(value.name);
    },$scope.selectedTypes);
    $scope.urlParams = $httpParamSerializer({search: $scope.search, type: $scope.selectedTypes});
  };

  $scope.updateAddress = function () {
    var searched = $scope.search;
    if ($scope.search.length > 0) {
      $scope.calculateUrlParams();
    }
    $http.get('address', {params: {search: $scope.search, type: $scope.selectedTypes}}).success(function(data) {
      if ($scope.search === searched) {
        $scope.addresses = data;
      }
    });
  };

  $scope.addressClicked = function (a) {
    a.extended_view = ! a.extended_view;
  }

  $scope.updateVersion = function() {
    $http.get('version').success(function(data) {
      var i = data.indexOf('.');
      $scope.version = i == -1 ? data : data.substring(0, i);
    });
  };

  $scope.updateVersion();
});
