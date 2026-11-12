    def get_effective_hash_keys(self):
        """Return the list of hash key fields to use for this account's streams.

        Uses this account's own ``hash_key`` override (stored under
        ``custom_properties["hash_key"]``) when set; otherwise falls back to
        the global ``m3u_hash_key`` CoreSettings value. A missing key means
        this account inherits the global default. Tokens are stripped and
        limited to the known hash field set.
        """
        custom_props = self.custom_properties or {}
        if isinstance(custom_props, dict):
            keys = parse_m3u_hash_keys(custom_props.get("hash_key"))
            if keys:
                return keys
        return parse_m3u_hash_keys(CoreSettings.get_m3u_hash_key())
