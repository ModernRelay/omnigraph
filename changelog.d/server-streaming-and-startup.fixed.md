- Export and change-baseline streams resume after slow clients release backpressure instead of failing after 250 ms. Startup readiness stays unavailable until all graph opening attempts finish, while healthy graphs may serve direct requests. Expired or abandoned serving transitions keep the affected graph closed without blocking other graphs’ transitions. See [deployment][server-release-corrections].

[server-release-corrections]: ../docs/user/deployment.md
