CREATE INDEX gid_geom_idx
  ON temp_geo_objects_attribute_arrays
  USING GIST (the_geom);