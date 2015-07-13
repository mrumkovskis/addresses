var addresses = angular.module('addresses', []);

addresses.controller('AddressesCtrl', function($scope, $http) {
  $http.get('version').success(function(data) {
  	var i = data.indexOf('.');
  	$scope.version = i == -1 ? data : data.substring(0, i);
  });
});