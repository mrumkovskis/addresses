var addresses = angular.module('addresses', []);

addresses.controller('AddressesCtrl', function($scope, $http, $interval) {
  $scope.search = '';
  $scope.addresses = [];
  $http.get('version').success(function(data) {
  	var i = data.indexOf('.');
  	$scope.version = i == -1 ? data : data.substring(0, i);
  });
  $scope.updateAddress = function () {
    var searched = $scope.search;
    $http.get('address', {params: {search: $scope.search}}).success(function(data) {
      if ($scope.search === searched) {
        $scope.addresses = data;
      }
    });
  }
});
