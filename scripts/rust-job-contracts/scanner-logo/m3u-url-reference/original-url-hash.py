    @classmethod
    def generate_hash_key(cls, name, url, tvg_id, keys=None, m3u_id=None, group=None,
                          account_type=None, stream_id=None):
        """Build a stable stream hash from the selected field list.

        ``keys`` must be a parsed field list (for example from
        ``get_effective_hash_keys()``). When omitted, the global M3U Hash
        Key CSV is parsed once here.
        """
        from apps.m3u.utils import parse_m3u_hash_keys

        if keys is None:
            keys = parse_m3u_hash_keys(CoreSettings.get_m3u_hash_key())

        # For XC accounts, use stream_id instead of url when 'url' is in the hash keys
        # This ensures credential/URL changes don't break stream identity
        effective_url = url
        use_stream_id = account_type == 'XC' and stream_id and 'url' in keys
        if use_stream_id:
            effective_url = stream_id

        stream_parts = {"name": name, "url": effective_url, "tvg_id": tvg_id, "m3u_id": m3u_id, "group": group}

        hash_parts = {key: stream_parts[key] for key in keys if key in stream_parts}

        # When using stream_id instead of URL, we MUST include m3u_id to prevent
        # collisions across different XC accounts (stream_id is only unique per account)
        if use_stream_id and 'm3u_id' not in hash_parts:
            hash_parts['m3u_id'] = m3u_id

        # Serialize and hash the dictionary
        serialized_obj = json.dumps(
            hash_parts, sort_keys=True
        )  # sort_keys ensures consistent ordering
        hash_object = hashlib.sha256(serialized_obj.encode())
        return hash_object.hexdigest()
