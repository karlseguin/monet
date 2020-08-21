F=

.PHONY: t
t:
	@monetdb status elixir_test > /dev/null || (monetdb create elixir_test && monetdb release elixir_test)
	mix test ${F}
